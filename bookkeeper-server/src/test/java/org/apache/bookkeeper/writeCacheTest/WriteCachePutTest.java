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
    private final boolean existingIds;
    private final boolean maxSegmentSize;

    public WriteCachePutTest(Object output, Integer capacity, long ledgerId, long entryId, long maxCacheSize, boolean defaultEntry, boolean fullCache, boolean existingIds, boolean maxSegmentSize) {
        if (output instanceof Class && Exception.class.isAssignableFrom((Class<?>) output)) {
            this.exceptionOutput = (Class<? extends Exception>) output;
        } else if (output instanceof Boolean) {
            this.booleanOutput = (Boolean) output;
        }

        this.fullCache = fullCache;
        this.entryId = entryId;
        this.ledgerId = ledgerId;
        this.maxCacheSize = maxCacheSize;
        this.existingIds = existingIds;
        this.maxSegmentSize = maxSegmentSize;

        if (capacity != null) {
            this.entry = Unpooled.buffer(capacity);
            entry.writeBytes(new byte[capacity]);
        }
        else if (defaultEntry) {
            /* Consider this configuration of entry buffer as non-valid because is empty */
            this.entry = Unpooled.EMPTY_BUFFER;
        } else
            this.entry = null;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {IllegalArgumentException.class, 1024, 1, -1, 2048, false, false, true, false},
                {IllegalArgumentException.class, 1024, -1, 1, 2048, false, false, true, false},
                {true, 0, 1, 1, 2048, true, false, true, false},
                {NullPointerException.class, null, 1, 1, 2048, false, false, true, false},
                {false, 2048, 1, 1, 1024, false, true, true, false},
                {true, 1024, 1, 1, 2048, false, false, true, false},
                {true, 1024, 1, 1, 2048, false, false, false, false},
                {NullPointerException.class, 1024, 1, 1, 2048, false, false, true, true} // test case to improve branch coverage from 62% to 75% and statement coverage from 96% to 97%
        });
    }

    @Before
    public void setUp() {
        if (maxSegmentSize)
            writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, maxCacheSize, entry.capacity()/2);
        else
            writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, maxCacheSize);
    }

    @Test
    public void test() {
        boolean check;
        ByteBuf tempBuf;
        ByteBuf tempBufOverwrite;

        if (fullCache) {
            entry.clear();
            entry.writeBytes(new byte[(int) (maxCacheSize + 1)]);
        }

        try {
            check = writeCache.put(ledgerId, entryId, entry);
            tempBuf = writeCache.get(ledgerId, entryId);

            if (!fullCache && existingIds) {
                /* Overwrite data */
                entry.clear();
                entry.writeBytes(new byte[(int) (128)]);

                writeCache.put(ledgerId, entryId, entry);
                tempBufOverwrite = writeCache.get(ledgerId, entryId);

                Assert.assertEquals(booleanOutput, check);
                Assert.assertNotEquals(tempBuf.readableBytes(), tempBufOverwrite.readableBytes());
            } else if (!fullCache) {
                /* This check is only for put test when cache is not full & the ids doesn't exist
                * because it will not create any new buckets */
                /* Check if the content is effectively written */
                Assert.assertEquals(booleanOutput, check);
                Assert.assertEquals(entry.readableBytes(), tempBuf.readableBytes());
            } else {
                /* Cache is full and ids doesn't exist so there aren't new created bucket to check */
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