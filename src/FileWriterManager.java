import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.BlockingQueue;

/**
 * This class is responsible for handling any writing of data to a given file
 * path. It associates a Metadata object, and creates a FileWriterWorker polling thread
 * (which polls data from an external resource, an internet connection input stream in our
 * case) and writes the data to disk. In addition, this class manages the updating of
 * the Metadata object accordingly on every successful chunk writing (to the file),
 * so we can keep track of the work done so far, in case we want to recover from
 * an error, to avoid re-downloading from the beginning.
 */
public class FileWriterManager {
    private String path;            // Path to save file
    private int chunkSize;          // Determined chunk size in bytes.
    private int contentLength;      // Content-Length of file in bytes.
    private Metadata metadata;      // Associated Metadata file.
    private RandomAccessFile file;
    private int fileChunksAmount;
    private IdcDm downloadManager;

    final private static String TEMP_SUFFIX = ".tmp";

    /**
     * Constructor for FileWriterManager.
     * @param path - given file path to write the data to.
     * @param contentLength - size of file in bytes.
     * @param chunkSize - the defined chunk size in bytes.
     * @param downloadManager - the creator of this instance.
     * @throws Exception
     */
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

        // Set the associated Metadata object to this FileWriterManager.
        this.metadata = this.setMetadata();
    }

    /**
     * This sets the Metadata file associated with the actual file we are
     * writing to disk. If the Metadata file doesn't exist, or could not
     * be deserialized, create a new Metadata object.
     * @return Metadata object
     */
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

        // If we can't deserialize the metadata, don't kill the program,
        // and don't output it as an error.
        // Start from the beginning (i.e. create a new Metadata object).
        } catch (IOException err) {
            System.out.println("Could not deserialize the metadata object. Starting over.");
        } catch (ClassNotFoundException err) {
            System.out.println("Metadata class not found. Starting over.");
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

    /**
     * Sets the file-pointer offset, and write from the specified
     * bytes array to the file. In addition, it updates the Metadata
     * object and writes it to disk in order to keep track of the
     * previous downloaded parts.
     * @param dataChunk - given DataChunk to write to disk.
     * @throws IOException
     */
    public void write(DataChunk dataChunk) throws IOException {
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

        // We may preform continuous renaming operations, as every new chunk is updated
        // to the disk metadata. Therefore, if the operating system failed to make
        // this atomic renaming for a single chunk, don't kill the program -
        // ignore it and continue.
        try {
            Files.move(tempMetadata.toPath(), realMetadata.toPath(), StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException ignored) {}

        // After successful procedure the correct metadata file is updated
        // and we can print the status to the user.
        int currentPercentage = this.metadata.getCurrentPercentage();
        if (currentPercentage > previousPercentage) {
            System.out.println("Downloaded " + currentPercentage + "%");
        }
    }

    /**
     * Closes the associated RandomAccessFile.
     */
    private void close() {
        try {
            this.file.close();
        } catch (IOException err) {
            this.downloadManager.kill(err);
        }
    }

    /**
     * Given a size in bytes, calculates the number of chunks required to download.
     * @param contentLength - given byte size.
     * @return the number of chunks.
     */
    private int calcChunksNum(int contentLength) {
        int div = (int) contentLength / this.chunkSize;
        return (contentLength % this.chunkSize == 0) ? div : div + 1;
    }

    /**
     * Getter for total amount of chunks in file.
     * @return number of chunks
     */
    public int getFileChunksAmount() {
        return this.fileChunksAmount;
    }

    /**
     * Getter to the amount of chunks that are left to download.
     * @return number of chunks
     */
    public int getChunksToFetchAmount() {
        return this.metadata.getChunksToFetch();
    }

    /**
     * Getter for content length size in bytes.
     * @return size
     */
    public int getContentLength() {
        return this.contentLength;
    }

    /**
     * Creates a FileWriterWorker thread. This thread is simply responsible
     * to fetch (poll) the chunks from a given blocking queue and pass it to
     * this manager instance to write it to disk.
     * @param bq - given Blocking Queue.
     * @return the thread
     */
    public Thread createWorkerThread(BlockingQueue<DataChunk> bq) {
        FileWriterWorker fileWorker = new FileWriterWorker(this, bq);
        return new Thread(fileWorker);
    }

    /**
     * Deletes the associated metadata file and closes the
     * RandomAccessFile.
     */
    public void cleanUp() throws Exception {
        String p = this.path + TEMP_SUFFIX;
        File file = new File(p);
        if (!file.delete()) {
            throw new Exception("Couldn't delete Metadata file: " + p);
        }

        if (this.file != null) {
            this.close();
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
