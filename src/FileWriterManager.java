import java.io.*;
import java.util.concurrent.BlockingQueue;

public class FileWriterManager {
    private String path;            // Path to save file
    private int chunkSize;          // Determined chunk size in bytes.
    private int contentLength;      // Content-Length of file in bytes.
    private Metadata metadata;      // Associated Metadata file.
    private RandomAccessFile file;
    private int fileChunksAmount;
    private IdcDm downloadManager;

    final private static String TEMP_SUFFIX = ".tmp";

    FileWriterManager(String path, int contentLength, int chunkSize, IdcDm downloadManager) throws Exception {
        this.path = path;
        this.chunkSize = chunkSize;
        this.contentLength = contentLength;
        this.downloadManager = downloadManager;
        this.fileChunksAmount = calcChunksNum(contentLength);

        try {
            this.file = new RandomAccessFile(path, "rw");
        } catch (FileNotFoundException err) {
            throw new Exception("Could not create file " + path);
        }

        // Set the Metadata object to this FileWriterManager.
        this.metadata = this.setMetadata();
    }

    private Metadata setMetadata() {
        try {
            // Deserialize the metadata object if already exists.
            File tempFile = new File(this.path + TEMP_SUFFIX);

            if (tempFile.exists()) {
                FileInputStream fileIn = new FileInputStream(tempFile);
                ObjectInputStream in = new ObjectInputStream(fileIn);
                Metadata metadata = (Metadata) in.readObject();
                in.close();
                fileIn.close();
                return metadata;
            }

        // If we can't deseriazlie the metadata, don't kill program,
        // and start from the beginning (i.e. create a new Metadata object).
        } catch (IOException err) {
            System.err.println("Could not deserialize the metadata object.");
        } catch (ClassNotFoundException err) {
            System.out.println("Metadata class not found.");
        }

        // Temp file does not exists or is corrupt, create a new Metadata object.
        return new Metadata(this.fileChunksAmount);
    }


    /**
     * Get the bitmap array which represent which chunks of the data have been already
     * written to disk. For example, if a file consist of 5 total chunks of data and
     * just the first and third were written to disk, the corresponding bitmap
     * is [1, 0, 1, 0, 0].
     * @return bitmap array
     */
    public boolean[] getBitMap() {
        return this.metadata.getBitMap();
    }

    public void write(DataChunk dataChunk) throws IOException {
        // Sets the file-pointer offset, and write from the specified
        // bytes array.

        int previousPercentage = this.metadata.getCurrentPercentage();

        try {
            // Seek to the relevant file offset and write the data as bytes array.
            this.file.seek(dataChunk.getOffset());
            this.file.write(dataChunk.getData());

            // Update the metadata object and serialize it to temp file.
            this.metadata.update(dataChunk.getChunkId());
            FileOutputStream fileOut = new FileOutputStream(this.path + TEMP_SUFFIX + "1");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);

            out.writeObject(this.metadata);
            out.close();
            fileOut.close();
        } catch (IOException err) {
            this.close();
            throw err;
        }

        // Rename the temporary metadata file.
        File tempMetadata = new File(this.path + TEMP_SUFFIX + "1"); // old name
        File realMetadata = new File(this.path + TEMP_SUFFIX); // new name
        if (tempMetadata.renameTo(realMetadata)) {
            // After successful procedure the correct metadata file is updated
            // and we can print the status to the user.
            int currentPercentage = this.metadata.getCurrentPercentage();
            if (currentPercentage > previousPercentage) {
                System.out.println("Downloaded " + currentPercentage + "%");
            }
        }
    }

    /**
     * Closes the associated RandomAccessFile.
     */
    private void close() {
        try {
            this.file.close();
        } catch (IOException err) {
            // TODO: throw error?
            System.err.println("Could not close file: " + err.toString());
        }
    }

    private int calcChunksNum(int contentLength) {
        int div = (int) contentLength / this.chunkSize;
        return (contentLength % this.chunkSize == 0) ? div : div + 1;
    }

    public int getFileChunksAmount() {
        return this.fileChunksAmount;
    }

    public int getContentLength() {
        return this.contentLength;
    }

    public Thread createWorkerThread(BlockingQueue<DataChunk> bq) {
        int numberOfChunksLeft = this.metadata.getChunksToFetch();
        FileWriterWorker fileWorker = new FileWriterWorker(this, bq, numberOfChunksLeft);
        return new Thread(fileWorker);
    }

    /**
     * Deletes the associated metadata file.
     */
    public void cleanUp() throws Exception {
        String p = this.path + TEMP_SUFFIX;
        File file = new File(p);
        if (!file.delete()) {
            throw new Exception("Couldn't delete Metadata file: " + p);
        }
    }

    /**
     * Terminate and propagate the error to parent Download Manager.
     * @param e - given error that is responsible for termination.
     */
    public void kill(Exception e) {
        this.downloadManager.kill(e);
    }
}
