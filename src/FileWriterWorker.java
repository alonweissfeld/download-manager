import java.io.IOException;
import java.util.concurrent.*;

/**
 * This class is responsible to poll any data from a given Blocking Queue
 * and pass it on to it's creator for writing the data.
 */
public class FileWriterWorker implements Runnable {
    private FileWriterManager fileManager;  // Associated FileWriterManager
    private BlockingQueue<DataChunk> bq;    // The queue from which we poll out data chunks.

    /**
     * Constructor for this Runnable.
     * @param fileManager - the creator of this thread.
     * @param bq - given Blocking Queue.
     */
    FileWriterWorker (FileWriterManager fileManager, BlockingQueue<DataChunk> bq) {
        this.bq = bq;
        this.fileManager = fileManager;
    }

    /**
     * Iterate n times of pulling data from the blocking queue, where n is
     * the amount of chunks that are left to poll from the queue, and
     * pass this data to this thread creator to write it to disk.
     */
    public void run() {
        int chunksLeft = this.fileManager.getChunksToFetchAmount();

        for (int i = 0; i < chunksLeft; i++) {
            try {
                // After 2 minutes of not getting any data down the pipeline,
                // something is wrong and we can timeout.
                DataChunk dataChunk = this.bq.poll(2, TimeUnit.MINUTES);
                if (dataChunk == null) {
                    this.kill(new Exception("Waited too long for a single chunk."));
                    return;
                }

                this.fileManager.write(dataChunk);
            } catch (IOException err) {
                this.kill(new Exception("FileWriterWorker failed to write data."));
                return; // Stop iteration to stop this thread work.
            } catch (InterruptedException e) {
                this.kill(e);
                return;
            }
        }
    }

    /**
     * Propagate a given exception to the creator of
     * this thread, which will eventually stop the program.
     * @param e - given exception.
     */
    private void kill(Exception e) {
        this.fileManager.kill(e);
    }
}
