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

import static org.mockito.Mockito.*;

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
    private boolean runOutBuffer;
    private long unpersistedBytesBound;
    private int bytesToWrite = 128;

    public BufferedChannelWriteTest(ByteBuf src, Object exception, FileStatus fileConstraints, boolean deallocatedBuffer, boolean runOutBuffer, long unpersistedBytesBound) throws IOException {
        this.src = src;
        if (fileConstraints != null)
            this.fileConstraints = fileConstraints;

        if (exception != null)
            this.exception = exception;

        this.deallocatedBuffer = deallocatedBuffer;
        this.originalPermissions = Files.getPosixFilePermissions(PATH);
        this.runOutBuffer = runOutBuffer;
        this.unpersistedBytesBound = unpersistedBytesBound;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        // test file size is 445 bytes
        return Arrays.asList(new Object[][] {
                {null, NullPointerException.class, FileStatus.READ_WRITE, false, false, 0},
                {Unpooled.directBuffer(1024), null, FileStatus.READ_WRITE, false, false, 0},
                /* Only read file channel, but we didn't flush anything to the file, so we didn't spot the exception */
                {Unpooled.directBuffer(1024), null, FileStatus.ONLY_READ, false, false, 0},
                {Unpooled.directBuffer(1024), ClosedChannelException.class, FileStatus.CLOSE_CHANNEL, false, false, 0},
                {Unpooled.directBuffer(1024), IllegalReferenceCountException.class, FileStatus.READ_WRITE, true, false, 0},    // Test case to simulate deallocated buffer
                {Unpooled.EMPTY_BUFFER, null, FileStatus.READ_WRITE, false, false, 0},
                /* Not testable on write because FileChannel throws an exception java.nio.file.AccessDeniedException */
                //{Unpooled.directBuffer(1024), AccessDeniedException.class, FileStatus.NO_PERMISSION, false},
                /* Adding test cases after JaCoCo report */
                {Unpooled.directBuffer(1024), null, FileStatus.READ_WRITE, false, true, 0},
                /* Now we flush to the file (open in only read), so we spot the exception */
                {Unpooled.directBuffer(1024), NonWritableChannelException.class, FileStatus.ONLY_READ, false, true, 0},
                {Unpooled.directBuffer(1024), null, FileStatus.READ_WRITE, false, false, 1},
                // Test case added after JaCoCo to cover else branch on if (unpersistedBytes.get() >= unpersistedBytesBound)
                {Unpooled.directBuffer(1024), null, FileStatus.READ_WRITE, false, false, 2048},
                // Test case added after Ba-Dua to cover one def-use -> now only 2 miss
                {Unpooled.EMPTY_BUFFER, null, FileStatus.READ_WRITE, false, false, 2048},
        });
    }

    @Before
    public void setUp() throws IOException {
        int capacity = 1024;
        setParam(fileConstraints);

        if (unpersistedBytesBound == 0)
            bufferedChannel = spy(new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity));
        else {
            /* Env 3 */
            if (unpersistedBytesBound == 1)
                bufferedChannel = spy(new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity, capacity, 1));
            else
                bufferedChannel = spy(new BufferedChannel(UnpooledByteBufAllocator.DEFAULT, fc, capacity, capacity, unpersistedBytesBound));
        }
        // bufferedChannel.writeBuffer has capacity 1024

        if (src != null && src.capacity() != 0)
            src.writeBytes(new byte[bytesToWrite]);  // write 128 byte in buffer src

        if (fileClose)
            fc.close();

        if (src != null && deallocatedBuffer)
            src.release();

        /* After JaCoCo report */
        /* We have to set up different environments to improve JaCoCo coverage:
         * 1) writeBuffer is writable -> no flush()
         * 2) writeBuffer run out of buffer space -> flush()
         * 3) init unpersistedBytesBound value */

        /* Env 2/3 */
        if (runOutBuffer || unpersistedBytesBound > 0)
            bufferedChannel.writeBuffer.writeBytes(new byte[capacity - 1]);
    }

    @Test
    public void test() {
        try {
            int prevContent = bufferedChannel.writeBuffer.readableBytes();
            long origFileSize = fc.size();
            long origPos = bufferedChannel.position();
            bufferedChannel.write(src);

            if (runOutBuffer && unpersistedBytesBound == 0) {
                /* Check content in write buffer */
                int flushedBytes = bufferedChannel.writeCapacity - prevContent; // bytes before the flush, the other remain in writeBuffer
                Assert.assertEquals(bytesToWrite - flushedBytes, bufferedChannel.writeBuffer.readableBytes());

                /* Now check the new file size, it is the full write buffer capacity */
                long newFileSize = origFileSize + bufferedChannel.writeBuffer.capacity();
                Assert.assertEquals(newFileSize, fc.size());
            } else if (unpersistedBytesBound > 0) {
                if (unpersistedBytesBound == 1) {
                    /* Check that writeBuffer is empty -> all the content has to be flushed to the file */
                    Assert.assertEquals(0, bufferedChannel.writeBuffer.readableBytes());

                    /* Check the new file size, it is the full write buffer capacity + all bytes to write in src buffer */
                    long newFileSize = origFileSize + prevContent + bytesToWrite;
                    Assert.assertEquals(newFileSize, fc.size());

                    /* Mutation kill: negated conditional on line 144 */
                    verify(bufferedChannel).forceWrite(false);
                    verify(bufferedChannel, times(2)).flush();
                }
                else {
                    Assert.assertNotEquals(0, bufferedChannel.writeBuffer.readableBytes());

                    /* Check the new file size, it is the full write buffer capacity, no flush() that is not needed */
                    if (src.capacity() != 0) {
                        long newFileSize = origFileSize + bufferedChannel.writeBuffer.capacity();
                        Assert.assertEquals(newFileSize, fc.size());

                        verify(bufferedChannel).flush();
                        Assert.assertTrue(bufferedChannel.unpersistedBytes.get() > 0);
                    }
                }
            } else {
                /* Check write buffer content: not yet flush bytes to the file */
                Assert.assertEquals(src.readableBytes(), bufferedChannel.writeBuffer.readableBytes());
            }

            /* Mutation kill: replaced addition with subtraction on line 135 */
            Assert.assertTrue(bufferedChannel.position >= origPos);
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
            if (!fileConstraints.equals(FileStatus.ONLY_READ))
                fc.truncate(0);
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
