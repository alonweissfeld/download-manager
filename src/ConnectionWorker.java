import java.io.*;
import java.net.*;
import java.util.concurrent.*;

public class ConnectionWorker implements Runnable {
    private int id;
    private URL url;
    private int rangeStart;
    private int rangeEnd;
    private boolean[] bitmap;
    private BlockingQueue<DataChunk> queue;
    private HttpURLConnection conn;
    private boolean includesLastChunk;
    private int chunksAmount;
    private int numOfWorkers;

    final private static int CHUNK_SIZE = 1024 * 4; // 4kb

    ConnectionWorker(int id, String url, int rangeStart, int rangeEnd, boolean[] bitmap, BlockingQueue<DataChunk> bq, boolean isLast, int chunksAmount, int numOfWorkers) {
        this.id = id;
        this.queue = bq;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
        this.bitmap = bitmap;
        this.includesLastChunk = isLast;
        this.chunksAmount = chunksAmount;
        this.numOfWorkers = numOfWorkers;

        try {
            this.url = new URL(url);
        } catch (MalformedURLException err) {
            System.err.println("Bad URL. " + err.toString());
        }

    }

    @Override
    public void run() {
        try {
            this.conn = (HttpURLConnection) this.url.openConnection();
            this.conn.setRequestMethod("GET");

            // Set Bytes range.
            String range = String.format("Bytes=%d-%d", this.rangeStart, this.rangeEnd);
            this.conn.setRequestProperty("Range", range);
            this.conn.connect();

            // Download data
            this.download();

        } catch (Exception err) {
            System.err.println(err.toString());
        }
    }

    private void download() {
        String msg = "[%d] Start downloading range (%d - %d) from:";
        System.out.println(String.format(msg, this.id, this.rangeStart, this.rangeEnd));
        System.out.println(this.url.toString());

        int fileOffset = 0;
        InputStream in = null;

        try {
            int len;
            int offset;
            int readUpTo = CHUNK_SIZE;
            in = this.conn.getInputStream();

            int startIdx = this.id * (this.bitmap.length / this.numOfWorkers);
            int endIdx = startIdx + this.chunksAmount;

            for (int i = startIdx; i < endIdx; i++) {
                len = 0; // the total number of bytes read into the buffer (current chunk)
                offset = 0;
                fileOffset = readUpTo * i;
                byte[] chunk = new byte[CHUNK_SIZE]; // allocate a chunk buffer.

                if (this.includesLastChunk && i == (endIdx - 1)) {
                    // This iteration will fetch the final chunk.
                    // Modify the buffered array to the relevant size.
                    chunk = new byte[(this.rangeEnd + 1) - fileOffset];
                    readUpTo = chunk.length;
                }

                if (this.bitmap[i]) {
                    // We have already downloaded and written this chunk. Skip to next group
                    // of bytes. We'll try to skip the whole chunk, but the skip method may,
                    // for a variety of reasons, end up skipping over some smaller number
                    // of bytes and therefore we need to make sure we've skipped the desired bytes.
                    while (offset < CHUNK_SIZE) {
                        len = (int) in.skip(readUpTo);
                        offset += len;
                    }
                    continue; // Continue to the next chunk.
                }

                // Reads up to len bytes of data from the input stream into an array of bytes.
                // An attempt is made to read as many as len bytes,
                // but a smaller number may be read.
                while (offset < readUpTo && len != -1) {
                    len = in.read(chunk, offset, readUpTo - offset);
                    offset += len;
                }

                // We filled up a single chunk.
                DataChunk dataChunk = new DataChunk(chunk, fileOffset, i);
                this.queue.put(dataChunk);
            }

            // We finished downloading for this thread.
            System.out.println(String.format("[%d] Finished downloading", this.id));

        } catch (Exception err) {
            System.err.println(err.toString());
        } finally {
            // Invoking the close() methods on the InputStream
            // of an URLConnection after a request may free network
            //resources associated with this instance.
            try {
                in.close();
            } catch (IOException err) {
                System.err.println("Cannot close URL input stream.");
            }
        }
    }

}
