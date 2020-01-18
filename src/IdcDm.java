import javax.rmi.ssl.SslRMIClientSocketFactory;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


public class IdcDm {
    final static int CHUNK_SIZE = 1024 * 4; // 4KB.
    final static int MINIMUM_BYTES_PER_CONNECTION = 1024 * 1000 * 10; // 10MB.

    public static void main(String[] args) {
        // Read inputs, decide on number of connections.
        if (args.length == 0) {
            String msg = "Usage:\n\tjava IdcDm URL|URL-LIST-FILE [MAX-CONCURRENT-CONNECTIONS]";
            System.err.println(msg);
            return;
        }

        String urlsArg = args[0];
        int connectionsNum = (args.length > 1) ? Integer.parseInt(args[1]) : 1;

        try {
            download(urlsArg, connectionsNum);
        } catch (Exception err) {
            System.err.println(err.toString());
            System.err.println("Download failed.");
            return;
        }

        System.out.println("Download succeeded.");
    }

    public static void download(String urlList, int connectionsNum) throws InterruptedException, IOException {
        List<String> urls = getUrls(urlList);
        long contentLength = getFileContentLength(urls.get(0));

        if (contentLength <= 0) {
            throw new Error("File's Content-Length is zero or unknown. Aborting.");
        }

        // Limit number of concurrent connections to download the file.
        connectionsNum = limitNumberOfConnections(contentLength, connectionsNum);
        String msg = (connectionsNum > 1) ? "Downloading using %d connections..." : "Downloading...";
        System.out.println(String.format(msg, connectionsNum));

        // Create a FileWriterManager that manages writing the data to disk and updating
        // the relevant metadata in order to support pause and resume while downloading.
        FileWriterManager fileWriter = new FileWriterManager(getPath(urls.get(0)), (int) contentLength, CHUNK_SIZE);

        //
        BlockingQueue<DataChunk> bq = new ArrayBlockingQueue<DataChunk>(1000);
        ExecutorService threadPool = Executors.newFixedThreadPool(connectionsNum);
        ConnectionWorker[] connectionWorkers = divideWorkers(urls, connectionsNum, fileWriter, bq);
        for (ConnectionWorker cw : connectionWorkers) {
            threadPool.execute(cw);
        }

        // Create a single FileWriterWorker thread that is responsible to fetch chunks of data
        // from the shared Blocking Queue.
        Thread fileWorkerThread = fileWriter.createWorkerThread(bq);

        threadPool.shutdown();
        try {
            fileWorkerThread.run();
        } catch (Exception e) {
            System.out.println("GOT ERR. " + e.toString());
        }

        awaitTermination(threadPool);
        fileWorkerThread.join();

        // We have finished downloading. clean up the metadata file.
        fileWriter.cleanUp();
    }

    /**
     * Returns an array of urls (multiple servers), in case we have
     * a file containing mirrors and not just one url.
     * @param str
     * @return
     */
    private static List<String> getUrls(String str) {
        File file = new File(str);
        List<String> result = new ArrayList<String>();

        if (file.isDirectory()) {
            throw new Error("Given url is a directory.");
        }

        if (!file.exists()) {
            // Given string is a url.
            result.add(str);
        } else {
            // We should have a file containing a list of urls.
            // Break it down to different mirrors and add each to the url list.
            BufferedReader reader;
            try {
                reader = new BufferedReader(new FileReader(str));
                String line = reader.readLine();

                while (line != null) {
                    result.add(line);
                    line = reader.readLine();
                }
            } catch (IOException e) {
                throw new Error("Can't read urls file.");
            }
        }

        // Remove any non-printable characters from all urls, in case they we're copied from
        // external sources to program arguments and may include a 'Zero Width Space' character.
        for (int i = 0; i < result.size(); i++) {
            result.set(i, result.get(i).replaceAll("[\\p{Cf}]", ""));
        }

        return result;
    }

    /**
     * Determines the full local path of a file we wish to download from the internet.
     * @param str
     * @return
     */
    private static String getPath(String str) {
        int idx = str.lastIndexOf("/");
        String workingDir = new File("").getAbsolutePath();
        return workingDir + str.substring(idx, str.length());
    }

    /**
     * Returns a file's content length from a given url.
     * @param urlString
     * @return
     */
    private static long getFileContentLength(String urlString) throws IOException {
        URL url;
        long length = 0;

        url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        length = conn.getContentLength();
        return length;
    }

    /**
     * A helper method to await termination for a given thread pool.
     * @param pool - the given ExecutorService thread pool.
     */
    private static void awaitTermination(ExecutorService pool) {
        try {
            pool.awaitTermination(2, TimeUnit.DAYS);
        } catch (InterruptedException err) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Given 'n' number of connections, divide the range of a file we wish to download
     * to different http byte ranges and create for each range a ConnectionWorker.
     * @param urls - linked list of url string, in case we use multiple servers.
     * @param n - number of desired connections to use.
     * @param fileWriter - the associated FileWriterManager, which manages the writing
     *                   operation to disk and handles any related metadata.
     * @param bq - given mutual blocking queue that is used as the pipe between a ConnectionWorker,
     *           that fetch chunks of data from the internet, to FileWriter, which polls from this queue
     *           and write the data to disk.
     * @return - returns an array of Connection Workers.
     */
    private static ConnectionWorker[] divideWorkers(List<String> urls, int n, FileWriterManager fileWriter, BlockingQueue<DataChunk> bq) {
        ConnectionWorker[] workers = new ConnectionWorker[n];

        boolean[] bitMap = fileWriter.getBitMap();
        int contentLength = fileWriter.getContentLength();
        int totalFileChunks = fileWriter.getFileChunksAmount();

        int rangeStart = 0;
        int chunksPerWorker = fileWriter.getFileChunksAmount() / n;

        for (int i = 0; i < n; i++) {
            int rangeEnd = rangeStart + (chunksPerWorker * CHUNK_SIZE) - 1;
            boolean isLastWorker = i == n - 1;
            if (isLastWorker) {
                chunksPerWorker = totalFileChunks - (i * chunksPerWorker);
                rangeEnd = (int) contentLength - 1;
            }

            // Assign each worker a url respectively to url list.
            String workerUrl = urls.get(i % urls.size());

            ConnectionWorker cw = new ConnectionWorker(i,
                    workerUrl, rangeStart, rangeEnd, bitMap, bq,
                    isLastWorker, chunksPerWorker, n);

            workers[i] = cw;
            rangeStart = rangeEnd + 1;
        }

        return workers;
    }

    /**
     * limits the number of connections such that
     * each worker fetches at least MINIMUM_BYTES_PER_CONNECTION.
     * @param contentLength - given content length, in bytes.
     * @param connectionsNum - given number of desired connections.
     * @return limited number of connections.
     */
    private static int limitNumberOfConnections(long contentLength, int connectionsNum) {
        if ((contentLength / connectionsNum) > MINIMUM_BYTES_PER_CONNECTION) {
            return connectionsNum;
        }

        int res = (int) (contentLength / MINIMUM_BYTES_PER_CONNECTION);

        // Let the user know why we limit the connections number, represented in MB.
        String msg = "Minimum range per connection is %dMB";
        System.out.println(String.format(msg, MINIMUM_BYTES_PER_CONNECTION / (1000 * 1024)));
        System.out.println("Optimizing connections number to " + res);

        return res;
    }

}
