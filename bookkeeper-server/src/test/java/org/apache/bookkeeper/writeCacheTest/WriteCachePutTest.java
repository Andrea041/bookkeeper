package org.apache.bookkeeper.writeCacheTest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.WriteCache;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class WriteCachePutTest {
    private final ByteBuf entry;
    private final long entryId;
    private final long ledgerId;
    private WriteCache writeCache;
    private final long maxCacheSize;
    private boolean booleanOutput;
    private Class<? extends Exception> exceptionOutput;
    private final boolean fullCache;

    public WriteCachePutTest(Object output, Integer capacity, long ledgerId, long entryId, long maxCacheSize, boolean defaultEntry, boolean fullCache) {
        if (output instanceof Class && Exception.class.isAssignableFrom((Class<?>) output)) {
            this.exceptionOutput = (Class<? extends Exception>) output;
        } else if (output instanceof Boolean) {
            this.booleanOutput = (Boolean) output;
        }

        this.fullCache = fullCache;
        this.entryId = entryId;
        this.ledgerId = ledgerId;
        this.maxCacheSize = maxCacheSize;

        if (capacity != null)
            this.entry = Unpooled.buffer(capacity);
        else if (defaultEntry) {
            this.entry = Unpooled.EMPTY_BUFFER;
        } else
            this.entry = null;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {IllegalArgumentException.class, 1024, 1, -1, 2048, false, false},
                {IllegalArgumentException.class, 1024, -1, 1, 2048, false, false},
                {true, 0, 1, 1, 2048, true, false},
                {NullPointerException.class, null, 1, 1, 2048, false, false},
                {false, 2048, 1, 1, 1024, false, true},
                {true, 1024, 1, 1, 2048, false, false}
        });
    }

    @Before
    public void setUp() {
        writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, maxCacheSize);
    }

    @Test
    public void test() {
        boolean check;
        ByteBuf tempBuf;

        if (fullCache)
            entry.writeBytes(new byte[(int) (maxCacheSize + 1)]);

        try {
            check = writeCache.put(ledgerId, entryId, entry);
            Assert.assertEquals(booleanOutput, check);

            tempBuf = writeCache.get(ledgerId, entryId);
            if (!fullCache) {
                /* This check is only for put test when cache is not full
                * because it will not create any new buckets */
                /* Check if the content is effectively written */
                Assert.assertEquals(entry.readableBytes(), tempBuf.readableBytes());
            } else {
                Assert.assertNull(tempBuf);
            }
        } catch (Exception e) {
            Assert.assertEquals(exceptionOutput, e.getClass());
        }
    }

    @After
    public void tearDown() {
        writeCache.close();
    }
}