package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.IllegalReferenceCountException;
import org.apache.bookkeeper.bookie.fileUtils.FileStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.NonWritableChannelException;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.mockito.Mockito.spy;

@RunWith(Parameterized.class)
public class BufferedChannelWriteTest {
    private ByteBuf src;
    private FileStatus fileConstraints;
    private Object exception;
    private FileChannel fc;
    private Set<PosixFilePermission> originalPermissions;
    private boolean fileClose;
    private BufferedChannel bufferedChannel;
    private boolean deallocatedBuffer;
    private final static Path PATH = Paths.get("src/test/java/org/apache/bookkeeper/bookie/fileUtils/test.txt");

    public BufferedChannelWriteTest(ByteBuf src, Object exception, FileStatus fileConstraints, boolean deallocatedBuffer) throws IOException {
        this.src = src;
        if (fileConstraints != null)
            this.fileConstraints = fileConstraints;

        if (exception != null)
            this.exception = exception;

        this.deallocatedBuffer = deallocatedBuffer;
        this.originalPermissions = Files.getPosixFilePermissions(PATH);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        // test file size is 445 bytes
        return Arrays.asList(new Object[][] {
                {null, NullPointerException.class, FileStatus.READ_WRITE, false},
                {Unpooled.directBuffer(1024), null, FileStatus.READ_WRITE, false},
                /* Not throws NonWritableChannelException, only write in writeBuffer */
                {Unpooled.directBuffer(1024), null, FileStatus.ONLY_READ, false},
                {Unpooled.directBuffer(1024), ClosedChannelException.class, FileStatus.CLOSE_CHANNEL, false},
                {Unpooled.directBuffer(1024), IllegalReferenceCountException.class, FileStatus.READ_WRITE, true},    // Test case to simulate deallocated buffer
                {Unpooled.EMPTY_BUFFER, null, FileStatus.READ_WRITE, false}
                /* Not testable on write because FileChannel throws an exception java.nio.file.AccessDeniedException */
                //{Unpooled.directBuffer(1024), AccessDeniedException.class, FileStatus.NO_PERMISSION, false},
        });
    }

    @Before
    public void setUp() throws IOException {
        int capacity = 1024;
        setParam(fileConstraints);

        bufferedChannel = spy(new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity));

        if (src != null && src.capacity() != 0) {
            src.writeBytes(new byte[128]);  // write 128 byte in buffer
        }

        if (fileClose)
            fc.close();

        if (src != null && deallocatedBuffer)
            src.release();
    }

    @Test
    public void test() {
        try {
            long outputSize = src.readableBytes() + fc.size();

            bufferedChannel.write(src);

            /* Check write buffer content: not yet flush bytes to the file */
            Assert.assertEquals(src.readableBytes(), bufferedChannel.writeBuffer.readableBytes());
        } catch (Exception e) {
            Assert.assertEquals(exception, e.getClass());
        }
    }

    private void setParam(FileStatus param) throws IOException {
        if (Files.notExists(PATH)) {
            Files.createFile(PATH);
        }

        switch(param) {
            case ONLY_READ:
                fc = FileChannel.open(PATH, StandardOpenOption.READ);
                break;
            case ONLY_WRITE:
                fc = FileChannel.open(PATH, StandardOpenOption.WRITE);
                break;
            case READ_WRITE:
                fc = FileChannel.open(PATH, StandardOpenOption.READ, StandardOpenOption.WRITE);
                break;
            case NO_PERMISSION:
                Set<PosixFilePermission> perms = PosixFilePermissions.fromString("---------");
                Files.setPosixFilePermissions(PATH, perms);
                fc = FileChannel.open(PATH, StandardOpenOption.CREATE);
                break;
            case CLOSE_CHANNEL:
                fc = FileChannel.open(PATH, StandardOpenOption.CREATE);
                fileClose = true;
                break;
        }
    }

    @After
    public void tearDown() throws IOException {
        if (fc != null && fc.isOpen()) {
            fc.close();
        }
        if (bufferedChannel != null) {
            bufferedChannel.close();
        }
        if (fileConstraints == FileStatus.NO_PERMISSION && originalPermissions != null) {
            Files.setPosixFilePermissions(PATH, originalPermissions);
        }
        if (src != null && !deallocatedBuffer) {
            src.release();
        }
    }
}
