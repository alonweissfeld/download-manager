public class DataChunk  {
    private byte[] data; // Actual byte data.
    private long offset; // Byte offset.
    private int chunkId; // Chunk position at Bitmap.

    /**
     * DataChunk constructor.
     */
    DataChunk(byte[] data, long offset, int chunkId) {
        this.data = data;
        this.offset = offset;
        this.chunkId = chunkId;
    }

    public byte[] getData() {
        return this.data;
    }

    public long getOffset() {
        return this.offset;
    }

    public int getChunkId() {
        return this.chunkId;
    }
}
