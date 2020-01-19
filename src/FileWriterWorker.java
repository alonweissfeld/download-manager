import java.io.IOException;
import java.util.concurrent.*;

public class FileWriterWorker implements Runnable {
    private FileWriterManager fileManager;  // Associated FileWriterManager
    private BlockingQueue<DataChunk> bq;    // The queue from which we poll out data chunks.
    private int chunksAmount;               // Expected number of chunks to poll from the queue.

    FileWriterWorker (FileWriterManager fileManager, BlockingQueue<DataChunk> bq, int chunksAmount) {
        this.bq = bq;
        this.fileManager = fileManager;
        this.chunksAmount = chunksAmount;
    }

    public void run() {
        for (int i = 0; i < this.chunksAmount; i++) {
            try {
                // After 1 minute of not getting any data, then we can timeout.
                DataChunk dataChunk = this.bq.poll(1, TimeUnit.MINUTES);
                if (dataChunk == null) {
                    this.kill(new Exception("Waited too long for a single chunk."));
                    return;
                }

                this.fileManager.write(dataChunk);
            } catch (IOException err) {
                this.kill(new Exception("FileWriterWorker failed to write data."));
                return;
            } catch (InterruptedException e) {
                // If we got an interrupt, that means that this thread has been
                // interrupted by a different thread asking it to close.
                return;
            }
        }
    }

    private void kill(Exception e) {
        this.fileManager.kill(e);
    }
}
