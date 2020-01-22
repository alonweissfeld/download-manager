/**
 *
 */
public class Metadata implements java.io.Serializable {
    private int length;
    private int chunksSoFar;
    private boolean[] bitMap;

    Metadata(int length) {
        // Represents how many chunks are downloaded and written to the
        // disk (i.e., updated in the metadata bit map) so far.
        this.chunksSoFar = 0;

        // On a new metadata object creation, we want to create
        // an array of 'false' in the length of the total chunk size.
        this.length = length;
        this.bitMap = new boolean[length];
    }

    public boolean[] getBitMap () {
        return this.bitMap;
    }

    public void update(int idx) {
        this.bitMap[idx] = true;
        this.chunksSoFar++;
    }

    public int getChunksToFetch() {
        return this.bitMap.length - this.chunksSoFar;
    }

    /**
     * Get the current integer percentage of completed (downloaded)
     * chunks out of the total number of chunks.
     * @return
     */
    public int getCurrentPercentage() {
        return (int) ((float) this.chunksSoFar / this.length * 100);
    }
}
