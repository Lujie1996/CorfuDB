package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.StreamsSnapshotWriter;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsLogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsSnapshotReader;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.SerializerException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Stream;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;


@Slf4j
public class ReplicationReaderWriterIT extends AbstractIT {
    static final String DEFAULT_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    static final int WRITER_PORT = DEFAULT_PORT + 1;
    static final String WRITER_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;
    static private final int START_VAL = 11;
    static private final int NUM_KEYS = 10;
    static private final int NUM_STREAMS = 2;
    static public final int NUM_TRANSACTIONS = 100;
    static final String PRIMARY_SITE_ID = "Cluster-Paris";

    Process server1;
    Process server2;

    // Connect with server1 to generate data
    CorfuRuntime srcDataRuntime = null;

    // Connect with server1 to read snapshot data
    CorfuRuntime readerRuntime = null;

    // Connect with server2 to write snapshot data
    CorfuRuntime writerRuntime = null;

    // Connect with server2 to verify data
    CorfuRuntime dstDataRuntime = null;

    HashMap<String, CorfuTable<Long, Long>> srcTables = new HashMap<>();
    HashMap<String, CorfuTable<Long, Long>> dstTables = new HashMap<>();
    HashMap<String, CorfuTable<Long, Long>> shadowTables = new HashMap<>();

    CorfuRuntime srcTestRuntime;
    HashMap<String, CorfuTable<Long, Long>> srcTestTables = new HashMap<>();

    CorfuRuntime dstTestRuntime;
    HashMap<String, CorfuTable<Long, Long>> dstTestTables = new HashMap<>();

    /*
     * the in-memory data for corfu tables for verification.
     */
    HashMap<String, HashMap<Long, Long>> srcHashMap = new HashMap<String, HashMap<Long, Long>>();
    HashMap<String, HashMap<Long, Long>> dstHashMap = new HashMap<String, HashMap<Long, Long>>();

    /*
     * store message generated by stream snapshot logreader and will play it at the writer side.
     */
    List<org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry> msgQ = new ArrayList<>();

    void setupEnv() throws IOException {
        // Start node one and populate it with data
        server1 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(DEFAULT_PORT)
                .setSingle(true)
                .runServer();

        server2 = new CorfuServerRunner()
                .setHost(DEFAULT_HOST)
                .setPort(WRITER_PORT)
                .setSingle(true)
                .runServer();

        CorfuRuntime.CorfuRuntimeParameters params = CorfuRuntime.CorfuRuntimeParameters
                .builder()
                .build();

        srcDataRuntime = CorfuRuntime.fromParameters(params);
        srcDataRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        srcDataRuntime.setTransactionLogging(true).connect();

        srcTestRuntime = CorfuRuntime.fromParameters(params);
        srcTestRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        srcTestRuntime.setTransactionLogging(true).connect();

        readerRuntime = CorfuRuntime.fromParameters(params);
        readerRuntime.parseConfigurationString(DEFAULT_ENDPOINT);
        readerRuntime.setTransactionLogging(true).connect();

        writerRuntime = CorfuRuntime.fromParameters(params);
        writerRuntime.parseConfigurationString(WRITER_ENDPOINT);
        writerRuntime.setTransactionLogging(true).connect();

        dstDataRuntime = CorfuRuntime.fromParameters(params);
        dstDataRuntime.parseConfigurationString(WRITER_ENDPOINT);
        dstDataRuntime.setTransactionLogging(true).connect();

        dstTestRuntime = CorfuRuntime.fromParameters(params);
        dstTestRuntime.parseConfigurationString(WRITER_ENDPOINT);
        dstTestRuntime.setTransactionLogging(true).connect();
    }

    public static void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt) {
        openStreams(tables, rt, NUM_STREAMS, Serializers.PRIMITIVE, false);
    }

    public static void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt, boolean shadow) {
        openStreams(tables, rt, NUM_STREAMS, Serializers.PRIMITIVE, shadow);
    }

    public static void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt, int num_streams) {
        openStreams(tables, rt, num_streams, Serializers.PRIMITIVE);
    }

    public static void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt, int num_streams,
                                   ISerializer serializer, boolean shadow) {
        for (int i = 0; i < num_streams; i++) {
            String name = "test" + i;
            if (shadow) {
                name = name + "_shadow";
            }
            CorfuTable<Long, Long> table = rt.getObjectsView()
                    .build()
                    .setStreamName(name)
                    .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                    })
                    .setSerializer(serializer)
                    .open();
            tables.put(name, table);
        }
    }

    public static void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt, int num_streams,
                                                 ISerializer serializer) {
        openStreams(tables, rt, num_streams, serializer, false);
    }


    public static void generateData(HashMap<String, CorfuTable<Long, Long>> tables,
                      HashMap<String, HashMap<Long, Long>> hashMap,
                      int numKeys, CorfuRuntime rt, long startval) {
        for (int i = 0; i < numKeys; i++) {
            for (String name : tables.keySet()) {
                hashMap.putIfAbsent(name, new HashMap<>());
                long key = i + startval;
                tables.get(name).put(key, key);
                log.trace("tail " + rt.getAddressSpaceView().getLogTail() + " seq " + rt.getSequencerView().query().getSequence());
                hashMap.get(name).put(key, key);
            }
        }
    }

    //Generate data with transactions and the same time push the data to the hashtable
    public static void generateTransactions(HashMap<String, CorfuTable<Long, Long>> tables,
                      HashMap<String, HashMap<Long, Long>> hashMap,
                      int numT, CorfuRuntime rt, long startval) {
        int j = 0;
        for (int i = 0; i < numT; i++) {
            rt.getObjectsView().TXBegin();
            for (String name : tables.keySet()) {
                hashMap.putIfAbsent(name, new HashMap<>());
                long key = j + startval;
                tables.get(name).put(key, key);
                log.trace("tail " + rt.getAddressSpaceView().getLogTail() + " seq " + rt.getSequencerView().query().getSequence());
                hashMap.get(name).put(key, key);
                j++;
            }
            rt.getObjectsView().TXEnd();
        }
        System.out.println("\ngenerate transactions num " + numT);
    }

    public static void verifyData(String tag, HashMap<String, CorfuTable<Long, Long>> tables, HashMap<String, HashMap<Long, Long>> hashMap) {
        System.out.println("\n" + tag);
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            HashMap<Long, Long> mapKeys = hashMap.get(name);
            System.out.println("table " + name + " key size " + table.keySet().size() +
                    " hashMap size " + mapKeys.size());

            assertThat(mapKeys.keySet().containsAll(table.keySet())).isTrue();
            assertThat(table.keySet().containsAll(mapKeys.keySet())).isTrue();
            assertThat(table.keySet().size() == mapKeys.keySet().size()).isTrue();

            for (Long key : mapKeys.keySet()) {
                assertThat(table.get(key)).isEqualTo(mapKeys.get(key));
            }
        }
    }

    public static void verifyTable(String tag, HashMap<String, CorfuTable<Long, Long>> tables, HashMap<String, CorfuTable<Long, Long>> hashMap) {
        System.out.println("\n" + tag);
        for (String name : hashMap.keySet()) {
            CorfuTable<Long, Long> table = tables.get(name);
            CorfuTable<Long, Long> mapKeys = hashMap.get(name);
            System.out.println("table " + name + " key size " + table.keySet().size() +
                    " hashMap size " + mapKeys.size());

            assertThat(mapKeys.keySet().containsAll(table.keySet())).isTrue();
            assertThat(table.keySet().containsAll(mapKeys.keySet())).isTrue();
            assertThat(table.keySet().size() == mapKeys.keySet().size()).isTrue();

            for (Long key : mapKeys.keySet()) {
                assertThat(table.get(key)).isEqualTo(mapKeys.get(key));
            }
        }
    }

    public static void verifyNoData(HashMap<String, CorfuTable<Long, Long>> tables) {
        for (CorfuTable table : tables.values()) {
            assertThat(table.keySet().isEmpty()).isTrue();
        }
    }


    void verifyTxStream(CorfuRuntime rt) {
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();

        IStreamView txStream = rt.getStreamsView().getUnsafe(ObjectsView.TRANSACTION_STREAM_ID, options);
        List<ILogData> dataList = txStream.remaining();
        System.out.println("\ndataList size " + dataList.size());
        for (ILogData data : txStream.remaining()) {
            System.out.println(data);
        }
    }

    public static void readLogEntryMsgs(List<org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) throws
            TrimmedException {
        LogReplicationConfig config = new LogReplicationConfig(streams);
        StreamsLogEntryReader reader = new StreamsLogEntryReader(rt, config);
        reader.setGlobalBaseSnapshot(Address.NON_ADDRESS, Address.NON_ADDRESS);

        for (int i = 0; i < NUM_TRANSACTIONS; i++) {
            LogReplicationEntry message = reader.read(UUID.randomUUID());

            if (message == null) {
                System.out.println("**********data message is null");
                assertThat(false).isTrue();
            } else {
                if (message == null) {
                    System.out.println("**********data message is null");
                    assertThat(false).isTrue();
                }

                //System.out.println("generate the message " + i);
                msgQ.add(message);
                //System.out.println("msgQ size " + msgQ.size());
            }
        }
    }

    public static void writeLogEntryMsgs(List<org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        org.corfudb.infrastructure.logreplication.LogReplicationConfig config = new LogReplicationConfig(streams);
        LogReplicationMetadataManager logReplicationMetadataManager = new LogReplicationMetadataManager(rt, 0, PRIMARY_SITE_ID);
        LogEntryWriter writer = new LogEntryWriter(rt, config, logReplicationMetadataManager);

        if (msgQ.isEmpty()) {
            System.out.println("msgQ is empty");
        }

        org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry data = msgQ.get(0);
        //writer.setTimestamp(data.metadata.getSnapshotTimestamp(), Address.NON_ADDRESS);

        for (org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry msg : msgQ) {
            writer.apply(msg);
        }
    }

    public static void printTails(String tag, CorfuRuntime rt0, CorfuRuntime rt1) {
        System.out.println("\n" + tag);
        System.out.println("src dataTail " + rt0.getAddressSpaceView().getLogTail());
        System.out.println("dst dataTail " + rt1.getAddressSpaceView().getLogTail());

    }

    @Test
    public void testTwoServersCanUp () throws IOException {
        System.out.println("\ntest start");
        setupEnv();

        openStreams(srcTables, srcDataRuntime);
        openStreams(srcTestTables, srcTestRuntime);
        openStreams(dstTables, dstDataRuntime);
        openStreams(dstTestTables, dstTestRuntime);

        // generate data at server1
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        // generate data at server2
        generateData(dstTables, dstHashMap, NUM_KEYS, dstDataRuntime, NUM_KEYS*2);

        verifyData("srctables", srcTables, srcHashMap);
        verifyData("srcTestTables", srcTestTables, srcHashMap);

        verifyData("dstCorfuTables", dstTables, dstHashMap);
        verifyData("dstTestTables", dstTestTables, dstHashMap);
        return;
    }

    public static void readSnapLogMsgs(List<org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams);
        StreamsSnapshotReader reader = new StreamsSnapshotReader(rt, config);
        int cnt = 0;

        reader.reset(rt.getAddressSpaceView().getLogTail());
        while (true) {
            cnt++;
            SnapshotReadMessage snapshotReadMessage = reader.read(UUID.randomUUID());
            for (org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry data : snapshotReadMessage.getMessages()) {
                msgQ.add(data);
            }

            if (snapshotReadMessage.isEndRead()) {
                break;
            }
        }
  }

    public static void writeSnapLogMsgs(List<org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams);
        LogReplicationMetadataManager logReplicationMetadataManager = new LogReplicationMetadataManager(rt, 0, PRIMARY_SITE_ID);
        StreamsSnapshotWriter writer = new StreamsSnapshotWriter(rt, config, logReplicationMetadataManager);

        if (msgQ.isEmpty()) {
            System.out.println("msgQ is empty");
        }

        long siteConfigID = msgQ.get(0).getMetadata().getTopologyConfigId();
        long snapshot = msgQ.get(0).getMetadata().getSnapshotTimestamp();
        logReplicationMetadataManager.setSrcBaseSnapshotStart(siteConfigID, snapshot);
        writer.reset(siteConfigID, snapshot);

        for (org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry msg : msgQ) {
            writer.apply(msg);
        }

        LogReplicationEntry msg = msgQ.get(msgQ.size() - 1);
        msg.getMetadata().setSnapshotSyncSeqNum(msg.getMetadata().getSnapshotSyncSeqNum() + 1);
        msg.getMetadata().setMessageMetadataType(MessageType.SNAPSHOT_TRANSFER_END);
        writer.snapshotTransferDone(msg);
    }

    void accessTxStream(Iterator iterator, int num) {

        int i = 0;
        while (iterator.hasNext() && i++ < num) {
            ILogData data = (ILogData) iterator.next();
            //System.out.println("entry address " + data.getGlobalAddress() );
        }
    }

    public static void clearTables(HashMap<String, CorfuTable<Long, Long>> tables) {
        for (CorfuTable<Long, Long> table : tables.values()) {
            table.clear();
        }
    }

    public static void trimAlone (long address, CorfuRuntime rt) {
        // Trim the log
        Token token = new Token(0, address);
        rt.getAddressSpaceView().prefixTrim(token);
        rt.getAddressSpaceView().gc();
        rt.getAddressSpaceView().invalidateServerCaches();
        rt.getAddressSpaceView().invalidateClientCache();
        System.out.println("\ntrim at " + token + " currentTail " + rt.getAddressSpaceView().getLogTail());
    }

    /**
     * enforce checkpoint entries at the streams.
     */
    public static Token ckStreamsAndTrim(CorfuRuntime rt, HashMap<String, CorfuTable<Long, Long>> tables) {
        MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
        for (CorfuTable map : tables.values()) {
            mcw1.addMap(map);
        }

        Token checkpointAddress = mcw1.appendCheckpoints(rt, "test");

        // Trim the log
        trimAlone(checkpointAddress.getSequence(), rt);
        return checkpointAddress;
    }

    public void trim(long address, CorfuRuntime rt) {
        Token token = ckStreamsAndTrim(rt, srcTables);

        Token trimMark = rt.getAddressSpaceView().getTrimMark();

        while (trimMark.getSequence() != (token.getSequence() + 1)) {
            System.out.println("trimMark " + trimMark + " trimToken " + token);
            trimMark = rt.getAddressSpaceView().getTrimMark();
        }

        rt.getAddressSpaceView().invalidateServerCaches();
        System.out.println("trim " + token);
    }

    void trimDelay() {
        try {
            while(msgQ.isEmpty()) {
                sleep(1);
            }
            trim(srcDataRuntime.getAddressSpaceView().getLogTail()- 2, srcDataRuntime);
        } catch (Exception e) {
            System.out.println("caught an exception " + e);
        }
    }

    void trimAloneDelay() {
        try {
            while(msgQ.isEmpty()) {
                sleep(1);
            }
            trimAlone(srcDataRuntime.getAddressSpaceView().getLogTail(), srcDataRuntime);
        } catch ( Exception e) {
            System.out.println("caught an exception " + e);
        }
    }

    /**
     * Generate some transactions, and start a txstream. Do a trim
     * To see if a trimmed exception happens
     */
    @Test
    public void testTrimmedExceptionForTxStream() throws IOException {
        final int reads = 10;
        setupEnv();
        openStreams(srcTables, srcDataRuntime);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_KEYS);

        // Open a tx stream
        IStreamView txStream = srcTestRuntime.getStreamsView().get(ObjectsView.TRANSACTION_STREAM_ID);
        long tail = srcDataRuntime.getAddressSpaceView().getLogTail();
        Stream<ILogData> stream = txStream.streamUpTo(tail);
        Iterator iterator = stream.iterator();

        accessTxStream(iterator, reads);
        Exception result = null;
        trim(tail, srcDataRuntime);

        try {
            accessTxStream(iterator, (int)tail);
        } catch (Exception e) {
            result = e;
            System.out.println("caught an exception " + e + " tail " + tail);
        } finally {
            assertThat(result).isInstanceOf(TrimmedException.class);
        }
    }


    @Test
    public void testOpenTableAfterTrimWithoutCheckpoint () throws IOException {
        final int offset = 20;
        setupEnv();
        openStreams(srcTables, srcDataRuntime);
        generateTransactions(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);

        trimAlone(srcDataRuntime.getAddressSpaceView().getLogTail() - offset, srcDataRuntime);

        try {
            CorfuTable<Long, Long> testTable = srcTestRuntime.getObjectsView()
                    .build()
                    .setStreamName("test0")
                    .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                    })
                    .setSerializer(Serializers.PRIMITIVE)
                    .open();
            long size = testTable.size();
        } catch (Exception e) {
            System.out.println("caught a exception " + e);
            assertThat(e).isInstanceOf(TrimmedException.class);
        }
    }

    @Test
    public void testTrimmedExceptionForLogEntryReader() throws IOException {
        setupEnv();
        openStreams(srcTables, srcDataRuntime);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_KEYS);
        long tail = srcDataRuntime.getAddressSpaceView().getLogTail();

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.submit(this::trimAloneDelay);
        Exception result = null;

        try {
            readLogEntryMsgs(msgQ, srcHashMap.keySet(), readerRuntime);
        } catch (Exception e) {
            result = e;
            System.out.println("msgQ size " + msgQ.size());
            System.out.println("caught an exception " + e + " tail " + tail);
        } finally {
            assertThat(result).isInstanceOf(TrimmedException.class);
        }
    }


    @Test
    public void testTrimmedExceptionForSnapshotReader() throws IOException, InterruptedException {
        setupEnv();

        openStreams(srcTables, srcDataRuntime, 1);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_KEYS);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        scheduledExecutorService.submit(this::trimDelay);

        long tail = srcDataRuntime.getAddressSpaceView().getLogTail();
        Exception result = null;

        try {
            readSnapLogMsgs(msgQ, srcHashMap.keySet(), readerRuntime);
        } catch (Exception e) {
            result = e;
            System.out.println("msgQ size " + msgQ.size());
            System.out.println("caught an exception " + e + " tail " + tail);
        } finally {
            assertThat(result).isInstanceOf(TrimmedException.class);
        }


        try {
            readSnapLogMsgs(msgQ, srcHashMap.keySet(), readerRuntime);
        } catch (Exception e) {
            result = e;
            System.out.println("msgQ size " + msgQ.size());
            System.out.println("second time caught an exception " + e + " tail " + tail);
        } finally {
            assertThat(result).isInstanceOf(TrimmedException.class);
        }

    }


    /**
     * Write to a corfu table and read SMRntries with streamview,
     * redirect the SMRentries to the second corfu server, and verify
     * the second corfu server contains the correct <key, value> pairs
     * @throws Exception
     */
    @Test
    public void testWriteSMREntries() throws Exception {
        // setup environment
        System.out.println("\ntest start");
        setupEnv();

        openStreams(srcTables, srcDataRuntime);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, NUM_KEYS);
        verifyData("after writing to server1", srcTables, srcHashMap);
        printTails("after writing to server1", srcDataRuntime, dstDataRuntime);

        //read streams as SMR entries
        StreamOptions options = StreamOptions.builder()
                .cacheEntries(false)
                .build();

        for (String name : srcHashMap.keySet()) {
            IStreamView srcSV = srcTestRuntime.getStreamsView().getUnsafe(CorfuRuntime.getStreamID(name), options);
            IStreamView dstSV = dstTestRuntime.getStreamsView().getUnsafe(CorfuRuntime.getStreamID(name), options);

            List<ILogData> dataList = srcSV.remaining();
            for (ILogData data : dataList) {
                OpaqueEntry opaqueEntry = OpaqueEntry.unpack(data);
                for (UUID uuid : opaqueEntry.getEntries().keySet()) {
                    for (SMREntry entry : opaqueEntry.getEntries().get(uuid)) {
                        dstSV.append(entry);
                    }
                }
            }
        }

        printTails("after writing to dst", srcDataRuntime, dstDataRuntime);
        openStreams(dstTables, writerRuntime);
        verifyData("after writing to dst", dstTables, srcHashMap);
    }

    @Test
    public void testPersistentTable() throws IOException {
        setupEnv();
        try {
            LogReplicationMetadataManager meta = new LogReplicationMetadataManager(writerRuntime, 0, PRIMARY_SITE_ID);
            meta.getLastProcessedLogTimestamp();
        } catch (Exception e) {
            e.getStackTrace();
            throw e;
        }
    }

    @Test
    public void testSnapTransfer() throws Exception {
        // setup environment
        System.out.println("\ntest start ok");
        setupEnv();

        openStreams(srcTables, srcDataRuntime);
        generateData(srcTables, srcHashMap, NUM_KEYS, srcDataRuntime, START_VAL);
        verifyData("after writing to src", srcTables, srcHashMap);

        //generate dump data at dst
        openStreams(dstTables, dstDataRuntime);
        generateData(dstTables, dstHashMap, NUM_KEYS, dstDataRuntime, START_VAL + NUM_KEYS);
        verifyData("after writing to dst", dstTables, dstHashMap);

        printTails("after writing to server1", srcDataRuntime, dstDataRuntime);


        //read snapshot from srcServer and put msgs into Queue
        readSnapLogMsgs(msgQ, srcHashMap.keySet(), readerRuntime);

        long dstEntries = msgQ.size()*srcHashMap.keySet().size();
        long dstPreTail = dstDataRuntime.getAddressSpaceView().getLogTail();

        //play messages at dst server
        writeSnapLogMsgs(msgQ, srcHashMap.keySet(), writerRuntime);

        long diff = dstDataRuntime.getAddressSpaceView().getLogTail() - dstPreTail;
        assertThat(diff == dstEntries);
        printTails("after writing to server2", srcDataRuntime, dstDataRuntime);

        //verify data with hashtable
        verifyTable("after snap write at dst", dstTables, srcTables);
        System.out.println("test done");
    }


    @Test
    public void testLogEntryTransferWithNoSerializer() throws IOException {
        // setup environment
        System.out.println("\ntest start ok");
        setupEnv();
        ISerializer serializer = new TestSerializer(Byte.MAX_VALUE);
        
        openStreams(srcTables, srcDataRuntime, NUM_STREAMS, serializer);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_TRANSACTIONS);

        HashMap<String, CorfuTable<Long, Long>> singleTables = new HashMap<>();
        singleTables.putIfAbsent("test0", srcTables.get("test0"));
        generateTransactions(singleTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_TRANSACTIONS);


        verifyData("after writing to src", srcTables, srcHashMap);

        printTails("after writing to server1", srcDataRuntime, dstDataRuntime);

        verifyTxStream(srcTestRuntime);

        //read snapshot from srcServer and put msgs into Queue
        readLogEntryMsgs(msgQ, srcHashMap.keySet(), readerRuntime);

        long dstEntries = msgQ.size();
        long dstPreTail = dstDataRuntime.getAddressSpaceView().getLogTail();

        //play messages at dst server
        writeLogEntryMsgs(msgQ, srcHashMap.keySet(), writerRuntime);

        long diff = dstDataRuntime.getAddressSpaceView().getLogTail() - dstPreTail;
        printTails("after writing to server2", srcDataRuntime, dstDataRuntime);

        //verify data with hashtable
        openStreams(dstTables, dstDataRuntime, NUM_STREAMS, serializer);

        Exception result = null;
        try {
            verifyData("after log writing at dst", dstTables, srcHashMap);
        } catch (Exception e) {
            System.out.println("caught an exception");
            result = e;
        } finally {
            assertThat(result instanceof SerializerException).isTrue();
        }
    }

    @Test
    public void testLogEntryTransferWithSerializer() throws IOException {
        // setup environment
        System.out.println("\ntest start ok");
        setupEnv();
        ISerializer serializer = new TestSerializer(Byte.MAX_VALUE);

        openStreams(srcTables, srcDataRuntime, NUM_STREAMS, serializer);
        openStreams(shadowTables, dstDataRuntime, NUM_STREAMS, serializer, true);
        generateTransactions(srcTables, srcHashMap, NUM_TRANSACTIONS, srcDataRuntime, NUM_TRANSACTIONS);

        //read snapshot from srcServer and put msgs into Queue
        readLogEntryMsgs(msgQ, srcHashMap.keySet(), readerRuntime);

        //play messages at dst server
        writeLogEntryMsgs(msgQ, srcHashMap.keySet(), writerRuntime);


        //verify data with hashtable
        openStreams(dstTables, dstDataRuntime, NUM_STREAMS, serializer);

        Serializers.registerSerializer(serializer);
        verifyData("after log writing at dst", dstTables, srcHashMap);
        Serializers.removeSerializer(serializer);
    }
}

