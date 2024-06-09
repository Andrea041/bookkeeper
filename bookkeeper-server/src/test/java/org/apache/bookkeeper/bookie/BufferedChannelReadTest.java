package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
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
import java.nio.channels.NonReadableChannelException;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BufferedChannelReadTest {
    private BufferedChannel bufferedChannel;
    private final ByteBuf dest;
    private final long pos;
    private final int length;
    private int intOutput;
    private Class<? extends Exception> exceptionOutput;
    private final FileStatus fileConstraints;
    private FileChannel fc;
    private Set<PosixFilePermission> originalPermissions;
    private boolean fileClose;
    private final static Path PATH = Paths.get("src/test/java/org/apache/bookkeeper/bookie/fileUtils/test.txt");

    public BufferedChannelReadTest(ByteBuf dest, long pos, int length, Object output, FileStatus fileConstraints) {
        this.dest = dest;
        this.pos = pos;
        this.length = length;
        this.fileConstraints = fileConstraints;

        if (output instanceof Class && Exception.class.isAssignableFrom((Class<?>) output))
            this.exceptionOutput = (Class<? extends Exception>) output;
        else
            this.intOutput = (int) output;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        // test file size is 445 bytes
        return Arrays.asList(new Object[][] {
                {null, 1, 1, NullPointerException.class, FileStatus.READ_WRITE},
                {Unpooled.directBuffer(1024), 1, 1, 444, FileStatus.READ_WRITE},
                {Unpooled.directBuffer(1024), 0, 1, 445, FileStatus.READ_WRITE},
                {Unpooled.directBuffer(1024), 1, -1, 0, FileStatus.READ_WRITE},
                {Unpooled.directBuffer(1024), 0, 0, 0, FileStatus.READ_WRITE},
                {Unpooled.directBuffer(1024), -1, 1, IllegalArgumentException.class, FileStatus.READ_WRITE},
                /* These tests spot an infinite loop if the buffer is empty */
                /*{Unpooled.EMPTY_BUFFER, 0, 1, IOException.class, FileStatus.READ_WRITE},
                {Unpooled.EMPTY_BUFFER, 0, 0, IOException.class, FileStatus.READ_WRITE},
                {Unpooled.EMPTY_BUFFER, 1, -1, 0, FileStatus.READ_WRITE},
                {Unpooled.EMPTY_BUFFER, -1, 1, IllegalArgumentException.class, FileStatus.READ_WRITE},
                {Unpooled.EMPTY_BUFFER, 1, 1, IllegalArgumentException.class, FileStatus.READ_WRITE},*/
                {Unpooled.directBuffer(1024), 0, 1, NonReadableChannelException.class, FileStatus.ONLY_WRITE},
                /* Not testable on read because FileChannel throws an exception java.nio.file.AccessDeniedException */
                //{Unpooled.directBuffer(1024), 0, 1, AccessDeniedException.class, FileStatus.NO_PERMISSION},
                {Unpooled.directBuffer(1024), 0, 1, ClosedChannelException.class, FileStatus.CLOSE_CHANNEL}
        });
    }

    @Before
    public void setUp() throws IOException {
        int capacity = 1024;
        setParam(fileConstraints);

        bufferedChannel = spy(new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity));

        /* We have to set up different environments to test read:
        * 1) writeBuffer is no empty
        * 2) readBuffer is no empty
        * 3) writeBuffer is null
        * 4) general case were we read to FileChannel */

        // implement switch
        // 4th case:
        bufferedChannel.writeBufferStartPosition.set(10);

        if (fileClose)
            fc.close();
    }

    @Test
    public void test() {
        int ret;
        try {
            ret = bufferedChannel.read(dest, pos, length);
            Assert.assertEquals(intOutput, ret);
        } catch (Exception e) {
            Assert.assertEquals(exceptionOutput, e.getClass());
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
                originalPermissions = Files.getPosixFilePermissions(PATH);
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
        if (dest != null) {
            dest.release();
        }
    }
}
