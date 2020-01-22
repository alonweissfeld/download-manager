/**
 * This class represents a Metadata object, which is associated
 * to a file we wish to download from the internet. Using a
 * bitmap array, it defines which parts in the file (chunks) have already
 * been taken care of (downloaded), and how many are left to download.
 * Every time we are done processing a chunk of data (writing it to the
 * file), we update the Metadata instance in it's bit map array - we
 * turn 'false' to 'true' in the designated index of the bitmap, which
 * correspond to the data chunk location (offset) in the file.
 * With that said, we serialize this instance to the disk in evey successful
 * chunk writing, in order to accurately recover from a failed download.
 * By deserializing the associated Metadata object we can continue to process
 * just the chunks of data that haven't been dealt with yet.
 * More than that, this object helps us with the implementation of
 * different parts of the system. By knowing how many chunks of data
 * we have left to download, we can program the readers and writer services
 * on how many chunks they are expected to deal with.
 */
public class Metadata implements java.io.Serializable {
    private int length;
    private int chunksSoFar;
    private boolean[] bitMap;

    /**
     * Constructor for the Metadata class.
     * @param length - the given length to create the associated
     *               bit map array.
     */
    Metadata(int length) {
        // Represents how many chunks are downloaded and written to the
        // disk (i.e., updated in the metadata bit map) so far.
        this.chunksSoFar = 0;

        // On a new metadata object creation, we want to create
        // an array of 'false' in the length of the total chunk size.
        this.length = length;
        this.bitMap = new boolean[length];
    }

    /**
     * Gets the bitmap array.
     * @return a boolean array.
     */
    public boolean[] getBitMap () {
        return this.bitMap;
    }

    /**
     * Updates from false to true in the array[index].
     * @param idx - given index.
     */
    public void update(int idx) {
        this.bitMap[idx] = true;
        this.chunksSoFar++;
    }

    /**
     * Returns how many chunks are left to deal with (in our case,
     * it's downloading the chunks from the internet). That means,
     * how many are still false in the bitmap array.
     * @return integer number of how many left.
     */
    public int getChunksToFetch() {
        return this.bitMap.length - this.chunksSoFar;
    }

    /**
     * Get the current integer percentage of completed (downloaded)
     * chunks out of the total number of chunks.
     * @return integer percentage.
     */
    public int getCurrentPercentage() {
        return (int) ((float) this.chunksSoFar / this.length * 100);
    }
}
