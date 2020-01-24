Alon Weissfeld
Matan Hazan

Source files:
IdcDm.java -
    Given user arguments, this is the where the program starts by creating the relevant services to download the file.
FileWriterManager -
    Manages any writing to the disk and handles metadata information updates to the file.
ConnectionWorker -
    Represents a thread that's responsible to read a specified byte range of the file from the internet.
FileWriterWorker -
    Represents a thread that's responsible to poll available data from a common blocking queue.
DataChunk -
    Represent a single data chunk (bytes array) of the file.
Metadata -
    Represent the file metadata information, serialized to the disk.
