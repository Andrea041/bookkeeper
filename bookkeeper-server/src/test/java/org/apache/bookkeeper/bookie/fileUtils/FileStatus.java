package org.apache.bookkeeper.bookie.fileUtils;

public enum FileStatus {
    ONLY_READ,
    ONLY_WRITE,
    READ_WRITE,
    NO_PERMISSION,
    CLOSE_CHANNEL,
    CREATE,
    EMPTY,
    NULL
}
