package org.corfudb.protocols.logprotocol;

import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class SMRGarbageInfoTest {
    private static UUID streamId1 = UUID.randomUUID();
    private static long address1_1 = 1L;
    private static int size1_1 = 100;
    private static int index1_1 = 0;

    private static UUID streamId2 = UUID.randomUUID();
    private static long address2_1 = 1L;
    private static int size2_1 = 200;
    private static int index2_1 = 2;
    private static long address2_2 = 2L;
    private static int size2_2 = 300;
    private static int index2_2 = 5;

    private void setUpInstance(SMRGarbageInfo gcInfo) {
        SMRRecordGarbageInfo smrEntryGarbageInfo1_1 = new SMRRecordGarbageInfo(address1_1, size1_1);
        SMRRecordGarbageInfo smrEntryGarbageInfo2_1 = new SMRRecordGarbageInfo(address2_1, size2_1);
        SMRRecordGarbageInfo smrEntryGarbageInfo2_2 = new SMRRecordGarbageInfo(address2_2, size2_2);

        gcInfo.add(streamId1, index1_1, smrEntryGarbageInfo1_1);
        gcInfo.add(streamId2, index2_1, smrEntryGarbageInfo2_1);
        gcInfo.add(streamId2, index2_2, smrEntryGarbageInfo2_2);
    }

    @Test
    public void testGetGarbageSize() {
        SMRGarbageInfo gcInfo = new SMRGarbageInfo();
        setUpInstance(gcInfo);
        Assert.assertEquals(size1_1 + size2_1 + size2_2, gcInfo.getGarbageSize());
    }

    @Test
    public void testEquals() {
        SMRGarbageInfo gcInfo = new SMRGarbageInfo();
        setUpInstance(gcInfo);

        SMRGarbageInfo empty = new SMRGarbageInfo();
        Assert.assertNotEquals(empty, gcInfo);

        SMRGarbageInfo other = new SMRGarbageInfo();
        other.add(streamId1, index1_1, new SMRRecordGarbageInfo(address1_1, size1_1));
        other.add(streamId2, index2_1, new SMRRecordGarbageInfo(address2_1, size2_1));
        other.add(streamId2, index2_2, new SMRRecordGarbageInfo(address2_2, size2_2));

        Assert.assertEquals(other, gcInfo);
    }

    @Test
    public void testMerge() {
        SMRGarbageInfo gcInfo = new SMRGarbageInfo();
        setUpInstance(gcInfo);

        SMRGarbageInfo other = new SMRGarbageInfo();

        long address1_2 = 3L;
        int size1_2 = 100;
        int index1_2 = 3;
        other.add(streamId1, index1_1, new SMRRecordGarbageInfo(address1_1, size1_1));
        other.add(streamId1, index1_2, new SMRRecordGarbageInfo(address1_2, size1_2));

        other.add(streamId2, index2_1, new SMRRecordGarbageInfo(address2_1, size2_1));

        UUID streamId3 = UUID.randomUUID();
        long address3_1 = 5L;
        int size3_1 = 100;
        int index3_1 = 0;
        other.add(streamId3, index3_1, new SMRRecordGarbageInfo(address3_1, size3_1));

        gcInfo.merge(other);

        SMRGarbageInfo expectedGCInfoAfterMerge = new SMRGarbageInfo();
        expectedGCInfoAfterMerge.add(streamId1, index1_1, new SMRRecordGarbageInfo(address1_1, size1_1));
        expectedGCInfoAfterMerge.add(streamId1, index1_2, new SMRRecordGarbageInfo(address1_2, size1_2));
        expectedGCInfoAfterMerge.add(streamId2, index2_1, new SMRRecordGarbageInfo(address2_1, size2_1));
        expectedGCInfoAfterMerge.add(streamId2, index2_2, new SMRRecordGarbageInfo(address2_2, size2_2));
        expectedGCInfoAfterMerge.add(streamId3, index3_1, new SMRRecordGarbageInfo(address3_1, size3_1));
        Assert.assertEquals(expectedGCInfoAfterMerge, gcInfo);
    }

}
