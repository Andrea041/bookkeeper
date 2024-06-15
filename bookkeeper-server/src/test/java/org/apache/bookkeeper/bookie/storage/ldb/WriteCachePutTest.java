package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.storage.ldb.cacheUtils.IdStatus;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@RunWith(Parameterized.class)
public class WriteCachePutTest {
    private final ByteBuf entry;
    private final IdStatus labelEntryId;
    private final IdStatus labelLedgerId;
    private WriteCache writeCache;
    private final long maxCacheSize;
    private boolean booleanOutput;
    private Class<? extends Exception> exceptionOutput;
    private final boolean fullCache;
    private final boolean maxSegmentSize;
    private int maxSegSize;
    private long ledgerId;
    private long entryId;

    public WriteCachePutTest(Object output,
                             Integer capacity,
                             IdStatus ledgerId,
                             IdStatus entryId,
                             long maxCacheSize,
                             boolean defaultEntry,
                             boolean fullCache,
                             boolean maxSegmentSize) {
        if (output instanceof Class && Exception.class.isAssignableFrom((Class<?>) output)) {
            this.exceptionOutput = (Class<? extends Exception>) output;
        } else if (output instanceof Boolean) {
            this.booleanOutput = (Boolean) output;
        }

        this.fullCache = fullCache;
        this.maxCacheSize = maxCacheSize;
        this.labelEntryId = entryId;
        this.labelLedgerId = ledgerId;
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

        if (maxSegmentSize && capacity != null)
            maxSegSize = entry.capacity()/2;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                //{IllegalArgumentException.class, 1024, IdStatus.EXISTING_ID, IdStatus.NEGATIVE_ID, 2048, false, false, true, false},  // reliability T1 - unexpected output (true instead of exception)
                {IllegalArgumentException.class, 1024, IdStatus.NEGATIVE_ID, IdStatus.EXISTING_ID, 2048, false, false, false},
                {true, 0, IdStatus.EXISTING_ID, IdStatus.EXISTING_ID, 2048, true, false, false},
                {NullPointerException.class, null, IdStatus.EXISTING_ID, IdStatus.EXISTING_ID, 2048, false, false, false},
                {true, 1024, IdStatus.EXISTING_ID, IdStatus.EXISTING_ID, 2048, false, false, false},
                {true, 1024, IdStatus.NOT_EXISTING_ID, IdStatus.NOT_EXISTING_ID, 2048, false, false, false},
                {true, 1024, IdStatus.EXISTING_ID, IdStatus.NOT_EXISTING_ID, 2048, false, false, false},
                // Test case added after JaCoCo results
                {false, 2048, IdStatus.EXISTING_ID, IdStatus.EXISTING_ID, 1024, false, true, false}, // test to simulate full cache
                {false, 1024, IdStatus.EXISTING_ID, IdStatus.EXISTING_ID, 2048, false, false, true}, // test case added to improve branch coverage from 62% to 75% and statement coverage from 96% to 97%
        });
    }

    @Before
    public void setUp() {
        if (maxSegmentSize)
            writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, maxCacheSize, maxSegSize);
        else
            writeCache = new WriteCache(UnpooledByteBufAllocator.DEFAULT, maxCacheSize);

        setUpValues();
    }

    @Test
    public void test() {
        boolean check;
        ByteBuf tempBuf;
        long cacheCount = 0;

        if (fullCache) {
            entry.clear();
            entry.writeBytes(new byte[(int) (maxCacheSize + 1)]);
            cacheCount = writeCache.count();
        }

        try {
            check = writeCache.put(ledgerId, entryId, entry);
            tempBuf = writeCache.get(ledgerId, entryId);

            if (!fullCache) {
                /* This check is only for put test when cache is not full & the ids doesn't exist
                * because it will not create any new buckets */
                Assert.assertEquals(booleanOutput, check);

                if (!maxSegmentSize) {
                    /* Check if the content is effectively written */
                    Assert.assertEquals(entry.readableBytes(), tempBuf.readableBytes());
                    Assert.assertNotEquals(cacheCount, writeCache.count()); // pit addition
                }
                /* Else: return false by put, so content not written in cache */
            } else {
                /* Cache is full so there aren't new created bucket to check */
                Assert.assertNull(tempBuf);
            }
        } catch (Exception e) {
            Assert.assertEquals(exceptionOutput, e.getClass());
        }
    }

    private void setUpValues() {
        switch (labelLedgerId) {
            case NEGATIVE_ID:
                this.ledgerId = -1;
                break;
            case NOT_EXISTING_ID:
                this.ledgerId = 1;
                break;
            case EXISTING_ID:
                this.ledgerId = 1;
                this.entryId = 1;

                if (entry != null)
                    writeCache.put(ledgerId, entryId, entry);
                break;
        }

        switch (labelEntryId) {
            case NEGATIVE_ID:
                this.entryId = -1;
                break;
            case NOT_EXISTING_ID:
                if (labelLedgerId == IdStatus.EXISTING_ID)
                    // change to implementation after jacoco
                    // this.entryId = entryId + 1;
                    this.entryId = entryId - 1;
                break;
            case EXISTING_ID:
                // nothing to do here, value is already assigned -> entryId only exist if already exist a ledgerId
                break;
        }


    }

    @After
    public void tearDown() {
        writeCache.close();
    }
}