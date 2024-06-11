package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBufAllocator;
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
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.mockito.Mockito.*;

@RunWith(Parameterized.class)
public class BufferedChannelConstructorTest {
    private BufferedChannel bufferedChannel;
    private ByteBufAllocator allocator;
    private FileChannel fc;
    private FileStatus fileConstraints;
    private boolean fileClose;
    private final static Path PATH = Paths.get("src/test/java/org/apache/bookkeeper/bookie/fileUtils/test.txt");
    private Class<? extends Exception> exceptionOutput;
    private int writeCapacity;
    private int readCapacity;
    private long unpersistedBytesBound;
    private Set<PosixFilePermission> originalPermissions;
    private boolean changeStartPosition;

    public BufferedChannelConstructorTest(ByteBufAllocator allocator,
                                          FileStatus fileConstraints,
                                          int writeCapacity,
                                          int readCapacity,
                                          long unpersistedBytesBound,
                                          Class<? extends Exception> exceptionOutput,
                                          boolean changeStartPosition) {
        this.allocator = allocator;
        this.fileConstraints = fileConstraints;
        this.writeCapacity = writeCapacity;
        this.readCapacity = readCapacity;
        this.unpersistedBytesBound = unpersistedBytesBound;

        if (exceptionOutput != null)
            this.exceptionOutput = exceptionOutput;

        this.changeStartPosition = changeStartPosition;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        // test file size is 445 bytes
        return Arrays.asList(new Object[][]{
                {UnpooledByteBufAllocator.DEFAULT, FileStatus.NULL, 1, 1, 1, NullPointerException.class, false},
                {null, FileStatus.CREATE, 1, 1, 1, NullPointerException.class, false},
                {UnpooledByteBufAllocator.DEFAULT, FileStatus.CREATE, 1, -1, 1, IllegalArgumentException.class, false},
                {UnpooledByteBufAllocator.DEFAULT, FileStatus.CREATE, 0, 1, -1, null, false},
                {UnpooledByteBufAllocator.DEFAULT, FileStatus.CREATE, -1, 0, 0, IllegalArgumentException.class, false},
                {UnpooledByteBufAllocator.DEFAULT, FileStatus.CLOSE_CHANNEL, 1, 1, 1, ClosedChannelException.class, false},
                {UnpooledByteBufAllocator.DEFAULT, FileStatus.CREATE, 1, 1, 1, null, false},
                {UnpooledByteBufAllocator.DEFAULT, FileStatus.CREATE, 1, 1, 1, null, false},
                {getInvalidAllocator(), FileStatus.CREATE, 1, 1, 1, NullPointerException.class, false},
                /* Not testable on constructor because FileChannel throws an exception java.nio.file.AccessDeniedException */
                //{UnpooledByteBufAllocator.DEFAULT, FileStatus.NO_PERMISSION, 1, 1, 1, AccessDeniedException.class}
                // Test added after PIT report to remove mutation on unchecked set(position) call -> set fc.position not 0
                {UnpooledByteBufAllocator.DEFAULT, FileStatus.CREATE, 1, 1, 1, null, true}
        });
    }

    @Before
    public void setUp() throws IOException {
        setParam(fileConstraints);

        if (fileClose)
            fc.close();
        if (changeStartPosition)
            fc.position(12);
    }

    @Test
    public void test() {
        try {
            bufferedChannel = spy(new BufferedChannel(allocator, fc, writeCapacity, readCapacity, unpersistedBytesBound));
            Assert.assertNotNull(bufferedChannel);

            /* Kill mutation that remove call to writeBufferStartPosition.set(position) */
            if (fc.position() > 0)
                Assert.assertNotEquals(0, bufferedChannel.writeBufferStartPosition.get());
            else
                Assert.assertEquals(0, bufferedChannel.writeBufferStartPosition.get());
        } catch (Exception e) {
            Assert.assertEquals(exceptionOutput, e.getClass());
        }
    }

    private void setParam(FileStatus param) throws IOException {
        if (Files.notExists(PATH)) {
            Files.createFile(PATH);
        }

        switch(param) {
            case CREATE:
                fc = FileChannel.open(PATH, StandardOpenOption.CREATE);
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
            case NULL:
                fc = null;
                break;
        }
    }

    private static ByteBufAllocator getInvalidAllocator() {
        ByteBufAllocator invalidAllocator = mock(ByteBufAllocator.class);

        when(invalidAllocator.directBuffer(anyInt())).thenReturn(null);
        return invalidAllocator;
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
    }
}
