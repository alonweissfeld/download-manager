import java.io.*;
import java.net.*;
import java.util.concurrent.*;

/**
 * This class is a Runnable thread that is responsible to download a specific byte
 * range of a file from a given url.
 * It uses a Blocking Queue, to pass any downloaded data forward for the services
 * that are responsible to write this data to the actual file.
 * It downloads the range from the internet by chunks, that is, each iteration we
 * call read() we fill up a complete chunk array of bytes. The size of the array
 * is determined by the chunkSize, and the number of times we expect to read
 * data was previously calculated by it's creator and is actually the number
 * of chunks that fit in the byte range for this thread.
 */
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
    final private static int READ_TIMEOUT_MS = 1000 * 20; // 20 seconds
    final private static int CONNECT_TIMEOUT_MS = 1000 * 25; // 25 seconds

    /**
     * Constructor for this class.
     * @param id - id of the thread, in relation to other Connection Workers.
     * @param url - the given url to download from.
     * @param rangeStart - the bytes range start.
     * @param rangeEnd - the bytes range end.
     * @param bitmap - the associated bitmap array.
     * @param bq - the associated Blocking Queue to pass any read() data forward.
     * @param isLast - indicates if this worker is responsible for the last range in the file.
     * @param chunksAmount - determines how many chunks this thread needs to download.
     * @param downloadManager - the creator of this thread.
     */
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
            this.downloadManager.kill(err);
        }

    }

    /**
     * Sets the chunk size according to the given size.
     * @param size - given size in bytes.
     */
    public void setChunkSize(int size) {
        this.chunkSize = size;
    }

    /**
     * The run method for this Runnable.
     * It creates a connection to the specified url, and downloads the
     * relevant byte range from the internet.
     */
    @Override
    public void run() {
        // Defensive check to avoid reading a redundant byte range.
        // In the case where the download have been paused (for any reason), and the number of
        // connections have been increased (on resume), we may have a situation where this
        // range have been covered already. The program intends to update the start range of the
        // thread for bytes already read, so we simply make sure that we didn't actually
        // read all this range already.
        if (this.rangeStart >= this.rangeEnd) {
            System.out.println(String.format("[%d] Finished. This range was already covered.", this.id));
            return;
        }

        try {
            String msg = "[%d] Start downloading range (%d - %d) from:\n%s";
            System.out.println(String.format(msg, this.id, this.rangeStart, this.rangeEnd, this.url.toString()));

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

            // We finished downloading for this thread.
            System.out.println(String.format("[%d] Finished downloading", this.id));
        } catch (Exception err) {
            this.downloadManager.kill(err);
        }
    }

    /**
     * Downloads the relevant byte range and pass this data on
     * to services who are responsible to write it to disk, by putting
     * this data in a mutual Blocking Queue. It knows how many data
     * is expected to be read according to the amount of data chunks that
     * are fit in the byte range.
     */
    private void download() {
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
