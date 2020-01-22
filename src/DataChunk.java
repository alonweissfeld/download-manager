/**
 * This class defines a chunk of data, with respect to it's position
 * in a file. That means, that it holds a bytes array of the actual
 * data, an offset indicating where to write this data in the file,
 * and a chunk id. The chunk id is the index of this chunk in a
 * bitmap array representing the file metadata. This index indicates
 * the location of the chunk in the file, corresponding to the bitmap
 * array index, while the offset is the actual bytes offset in the file.
 */
public class DataChunk  {
    private byte[] data; // Actual byte data.
    private long offset; // Byte offset.
    private int chunkId; // Chunk position at Bitmap.

    /**
     * DataChunk constructor. Represent a chunk of data pipelined
     * between a reader and writer services, designated to be written
     * to a file.
     * @param data - given byte array of data
     * @param offset - given offset, indicates in which offset to write
     * @param chunkId - represents the associated chunk id, which corresponds
     * to the index of this chunk in a bitmap representing the file.
     */
    DataChunk(byte[] data, long offset, int chunkId) {
        this.data = data;
        this.offset = offset;
        this.chunkId = chunkId;
    }

    /**
     * Gets the data.
     * @return data bytes array.
     */
    public byte[] getData() {
        return this.data;
    }

    /**
     * Gets the offset.
     * @return long offset position.
     */
    public long getOffset() {
        return this.offset;
    }

    /**
     * Gets the chunk id.
     * @return integer chunk id.
     */
    public int getChunkId() {
        return this.chunkId;
    }
}
