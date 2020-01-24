/**
 * Alon Weissfeld, 307941310
 * Matan Hazan, 203529136
 */

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Main class for the Download Manager.
 * This program downloads a file from the internet, by a given
 * url or by a given path to multiple urls. Also, it can use
 * different number of connections if the user wish to use it.
 * On top of that, we initialize an instance of this class in order
 * to pass information (mainly error handling) between the readers
 * and writers of the data.
 */
public class IdcDm {
    private String urlPath;
    private int connectionsAmount;
    private ExecutorService threadPool;

    // Sets the constants used throughout the program.
    // Using 64kb per chunk seems like a good considering memory usage.
    // For the minimum bytes per connection, we want to give an upper bound on how
    // many connections are used. If the user wants to download a 2MB file, it doesn't
    // make sense to open many connections. Therefore, we initialize a minimum size
    // for each connection.
    // Finally, the queue capacity sets the maximum number of elements that can be hold
    // for our consumers and producers data blocking queue. Since we are polling data from
    // the queue immediately when it's available, there is no sense in leaving them in the
    // queue. Therefore, this is just an arbitrary defensive integer.
    final static int CHUNK_SIZE = 1024 * 64; // 64KB.
    final static int MINIMUM_BYTES_PER_CONNECTION = 1024 * 1000; // 1MB.
    final static int QUEUE_CAPACITY = 1000;

    public static void main(String[] args) {
        // Read inputs, decide on number of connections.
        if (args.length == 0) {
            String msg = "Usage:\n\tjava IdcDm URL|URL-LIST-FILE [MAX-CONCURRENT-CONNECTIONS]";
            System.err.println(msg);
            return;
        }

        String urlPath = args[0];
        int connectionsAmount = (args.length > 1) ? Integer.parseInt(args[1]) : 1;

        IdcDm downloadManager = new IdcDm(urlPath, connectionsAmount);

        try {
            downloadManager.download();
            System.out.println("Download succeeded.");
        } catch (Exception err) {
            downloadManager.kill(err);
        }
    }

    /**
     * Constructor for this class.
     * @param path - given path to download url. Could be a path
     *             to a file in the system if it contains multiple urls.
     * @param n - number of connections the user wish to user.
     */
    IdcDm(String path, int n) {
        this.urlPath = path;
        this.connectionsAmount = n;
    }

    /**
     * Downloads the desired file from the internet.
     * It initiates a FileWriterManager, which is responsible to
     * handle any writing of the data to the file, and update any
     * metadata accordingly. With that, it creates connection workers
     * threads to download different byte ranges of the file simultaneously.
     * @throws Exception
     */
    public void download() throws Exception {
        List<String> urls = getUrls(this.urlPath);
        long contentLength = getFileContentLength(urls.get(0));

        if (contentLength <= 0) {
            throw new Exception("File's Content-Length is zero or unknown. Aborting.");
        }

        // Limit number of concurrent connections to download the file.
        int connectionsNum = limitNumberOfConnections(contentLength, this.connectionsAmount);
        String msg = (connectionsNum > 1) ? "Downloading using %d connections..." : "Downloading...";
        System.out.println(String.format(msg, connectionsNum));

        // Create a FileWriterManager that manages writing the data to disk and updating
        // the relevant metadata in order to support pause and resume while downloading.
        FileWriterManager fileWriter = new FileWriterManager(getPath(urls.get(0)), (int) contentLength, CHUNK_SIZE, this);

        // Create a fixed-sized array that holds elements inserted by producers and extracted by consumers.
        BlockingQueue<DataChunk> bq = new ArrayBlockingQueue<DataChunk>(QUEUE_CAPACITY);
        ExecutorService threadPool = Executors.newFixedThreadPool(connectionsNum + 1);
        ConnectionWorker[] connectionWorkers = this.divideWorkers(urls, fileWriter, bq);
        for (ConnectionWorker cw : connectionWorkers) {
            threadPool.execute(cw);
        }

        // Create a single FileWriterWorker thread that is responsible to fetch chunks of data
        // from the shared Blocking Queue.
        Thread fileWorkerThread = fileWriter.createWorkerThread(bq);
        threadPool.execute(fileWorkerThread);

        // Set the thread pool on this Download Manager instance.
        this.setThreadPool(threadPool);

        threadPool.shutdown();
        // We assume that this program wouldn't download a file longer then a single day
        this.awaitTermination(1, TimeUnit.DAYS);

        // We have finished downloading. clean up the metadata file.
        fileWriter.cleanUp();
    }

    /**
     * Returns an array of urls (multiple servers), in case we have
     * a file containing mirrors and not just one url.
     * @param str
     * @return
     */
    private static List<String> getUrls(String str) throws Exception {
        File file = new File(str);
        List<String> result = new ArrayList<String>();

        if (file.isDirectory()) {
            throw new Exception("Given url is a directory.");
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
                throw new Exception("Can't read urls file.");
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
     * @param time - time number
     * @param unit - TimeUnit
     */
    private void awaitTermination(int time, TimeUnit unit) {
        try {
            this.threadPool.awaitTermination(time, unit);
        } catch (InterruptedException err) {
            this.threadPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Given 'n' number of connections, divide the range of a file we wish to download
     * to different http byte ranges and create for each range a ConnectionWorker.
     * @param urls - linked list of url string, in case we use multiple servers.
     * @param fileWriter - the associated FileWriterManager, which manages the writing
     *                   operation to disk and handles any related metadata.
     * @param bq - given mutual blocking queue that is used as the pipe between a ConnectionWorker,
     *           that fetch chunks of data from the internet, to FileWriter, which polls from this queue
     *           and write the data to disk.
     * @return - returns an array of Connection Workers.
     */
    private ConnectionWorker[] divideWorkers(List<String> urls, FileWriterManager fileWriter, BlockingQueue<DataChunk> bq) {
        int n = this.connectionsAmount;
        ConnectionWorker[] workers = new ConnectionWorker[n];

        boolean[] bitMap = fileWriter.getBitMap();
        int contentLength = fileWriter.getContentLength();
        int totalFileChunks = fileWriter.getFileChunksAmount();

        int rangeStart = 0;
        int chunksPerWorker = fileWriter.getFileChunksAmount() / n;

        for (int i = 0; i < n; i++) {
            int rangeEnd = rangeStart + (chunksPerWorker * CHUNK_SIZE) - 1;

            // Since we may have downloaded some data already for this
            // worker range (on pause and resume), we want to trim the bytes
            // range to a more bounded range avoiding reading data that was
            // already read.
            int boundedRangeStart = rangeStart;
            int bitMapStartIdx = rangeStart / CHUNK_SIZE;
            int boundedChunksAmount = chunksPerWorker;

            // The last chunk of the last worker is more likely to be less then the fixed
            // chunk size. Adjust the bytes range accordingly.
            boolean isLastWorker = i == n - 1;
            if (isLastWorker) {
                boundedChunksAmount = totalFileChunks - (i * chunksPerWorker);
                rangeEnd = (int) contentLength - 1;
            }

            // As explained above, update the start range to the exact relevant value.
            for (int j = bitMapStartIdx; j < bitMapStartIdx + chunksPerWorker; j++) {
                if (bitMap[j]) {
                    // We don't want to include this chunk in the range. Increment the
                    // bounded range and decrease the amount of chunks for this worker.
                    boundedRangeStart += CHUNK_SIZE;
                    boundedChunksAmount--;
                } else {
                    break;
                }
            }

            // Assign each worker a url respectively to the url list.
            String workerUrl = urls.get(i % urls.size());

            ConnectionWorker cw = new ConnectionWorker(i,
                    workerUrl, boundedRangeStart, rangeEnd, bitMap, bq,
                    isLastWorker, boundedChunksAmount, this);

            cw.setChunkSize(CHUNK_SIZE);
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

    private void setThreadPool(ExecutorService pool) {
        this.threadPool = pool;
    }

    /**
     * Shutdown running processes and let the user know that the download have failed.
     * The first exception that raise this method - ends the program.
     * @param e - any information why the download have failed.
     */
    public synchronized void kill(Exception e) {
        if (this.threadPool != null) {
            this.threadPool.shutdownNow();
        }

        System.err.println(e.getMessage());
        System.err.println("Download failed.");

        // Tasks may ignore the interrupts created by shutDownNow.
        System.exit(1);
    }

}
