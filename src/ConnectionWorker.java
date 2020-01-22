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
    private IdcDm downloadManager;
    private int chunkSize;

    // Sets the default constants.
    final private static int CHUNK_SIZE = 1024 * 64; // 64kb
    final private static int READ_TIMEOUT_MS = 1000 * 4; // 20 seconds
    final private static int CONNECT_TIMEOUT_MS = 1000 * 25; // 25 seconds

    ConnectionWorker(int id, String url, int rangeStart, int rangeEnd, boolean[] bitmap,
                     BlockingQueue<DataChunk> bq, boolean isLast, int chunksAmount,
                     IdcDm downloadManager) {
        this.id = id;
        this.queue = bq;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
        this.bitmap = bitmap;
        this.includesLastChunk = isLast;
        this.chunksAmount = chunksAmount;
        this.downloadManager = downloadManager;
        this.chunkSize = CHUNK_SIZE;

        try {
            this.url = new URL(url);
        } catch (MalformedURLException err) {
            System.err.println("Bad URL. " + err.toString());
        }

    }

    /**
     * Sets the chunk size according to the given size.
     * @param size - given size in bytes.
     */
    public void setChunkSize(int size) {
        this.chunkSize = size;
    }

    @Override
    public void run() {
        try {
            this.conn = (HttpURLConnection) this.url.openConnection();
            this.conn.setRequestMethod("GET");

            // Set Bytes range.
            String range = String.format("Bytes=%d-%d", this.rangeStart, this.rangeEnd);
            this.conn.setRequestProperty("Range", range);
            this.conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
            this.conn.setReadTimeout(READ_TIMEOUT_MS);
            this.conn.connect();

            // Download data
            this.download();

        } catch (Exception err) {
            this.downloadManager.kill(err);
        }
    }

    private void download() {
        String msg = "[%d] Start downloading range (%d - %d) from:\n%s";
        System.out.println(String.format(msg, this.id, this.rangeStart, this.rangeEnd, this.url.toString()));

        int fileOffset = 0;
        InputStream in = null;

        try {
            int len;
            int offset;
            int readUpTo = this.chunkSize;
            in = this.conn.getInputStream();

            int startIdx = this.rangeStart / this.chunkSize;
            int endIdx = startIdx + this.chunksAmount;

            for (int i = startIdx; i < endIdx; i++) {
                len = 0; // the total number of bytes read into the buffer (current chunk)
                offset = 0;
                fileOffset = readUpTo * i;
                byte[] chunk = new byte[this.chunkSize]; // allocate a chunk buffer.

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
                    while (offset < this.chunkSize) {
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
            // Propagate the error to the creator of this thread.
            this.downloadManager.kill(err);
        } finally {
            // Invoking the close() methods on the InputStream
            // of an URLConnection after a request may free network
            // resources associated with this instance.
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException err) {
                // If we're trying to close this input stream, we have
                // finished downloading with this thread. Therefore, don't propagate
                // this error, but print this information to the screen.
               System.err.println(err.toString());
            }
        }
    }
}
