import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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
                // After 2 minutes of not getting any data (which is just
                // a single chunk of ~4kb) then we can timeout.
                DataChunk dataChunk = this.bq.poll(3, TimeUnit.MINUTES);
                if (dataChunk == null) {
                    throw new Error("Waited too long for a single chunk.");
                }

                this.fileManager.write(dataChunk);
            } catch (InterruptedException | IOException err) {
                err.printStackTrace();
                throw new Error("FileWriterWorker failed to write data.");
            }

        }
    }
}
