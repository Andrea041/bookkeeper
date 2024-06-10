package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.spy;

@RunWith(Parameterized.class)
public class WriteCacheGetTest {
    private final long ledgerId;
    private final long entryId;
    private Class<? extends Exception> exceptionOutput;
    private ByteBuf outputBuffer;
    private WriteCache writeCache;
    /* this attribute is used to simulate the existence of ledgerId and entryId */
    private final boolean existingIds;

    public WriteCacheGetTest(long ledgerId, long entryId, Object output, boolean existingIds) {
        if (output instanceof Class && Exception.class.isAssignableFrom((Class<?>) output)) {
            this.exceptionOutput = (Class<? extends Exception>) output;
            this.outputBuffer = Unpooled.EMPTY_BUFFER;
        } else if (output instanceof ByteBuf) {
            this.outputBuffer = (ByteBuf) output;
        } else if (output == null) {
            this.outputBuffer = Unpooled.EMPTY_BUFFER;
        }

        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.existingIds = existingIds;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        int capacity = 1024;
        ByteBuf entry = Unpooled.buffer(capacity);
        entry.writeBytes(new byte[capacity]);

        return Arrays.asList(new Object[][] {
                {1, -1, IllegalArgumentException.class, true},
                {0, 1, null, false},    // not exception = legal argument, but not existing ids
                {-1, 0, IllegalArgumentException.class, true},
                {1, 1, entry, true}
        });
    }

    @Before
    public void setUp() {
        long maxCacheSize = 1024;
        writeCache = spy(new WriteCache(UnpooledByteBufAllocator.DEFAULT, maxCacheSize));
    }

    @Test
    public void test() {
        ByteBuf entryRes;

        try {
            if (existingIds)
                writeCache.put(ledgerId, entryId, outputBuffer);

            entryRes = writeCache.get(ledgerId, entryId);

            if (existingIds) {
                Assert.assertEquals(outputBuffer, entryRes);
                Assert.assertEquals(outputBuffer.readableBytes(), entryRes.readableBytes());
            } else
                Assert.assertNull(entryRes);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(exceptionOutput, e.getClass());
        }
    }

    @After
    public void tearDown() {
        writeCache.close();
    }
}
