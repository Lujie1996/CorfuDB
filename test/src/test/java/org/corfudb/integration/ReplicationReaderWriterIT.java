package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.message.LogReplicationEntry;
import org.corfudb.logreplication.receive.LogEntryWriter;
import org.corfudb.logreplication.receive.PersistedWriterMetadata;
import org.corfudb.logreplication.receive.StreamsSnapshotWriter;
import org.corfudb.logreplication.send.SnapshotReadMessage;
import org.corfudb.logreplication.send.StreamsLogEntryReader;
import org.corfudb.logreplication.send.StreamsSnapshotReader;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class ReplicationReaderWriterIT extends AbstractIT {
    static final String DEFAULT_ENDPOINT = DEFAULT_HOST + ":" + DEFAULT_PORT;
    static final int WRITER_PORT = DEFAULT_PORT + 1;
    static final String WRTIER_ENDPOINT = DEFAULT_HOST + ":" + WRITER_PORT;
    static private final int START_VAL = 11;
    static private final int NUM_KEYS = 10;
    static private final int NUM_STREAMS = 2;
    static private final int NUM_TRANSACTIONS = 10;

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

    Random random = new Random();
    HashMap<String, CorfuTable<Long, Long>> srcTables = new HashMap<>();
    HashMap<String, CorfuTable<Long, Long>> dstTables = new HashMap<>();

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
     * store message generated by streamsnapshot reader and will play it at the writer side.
     */
    List<LogReplicationEntry> msgQ = new ArrayList<>();

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
        writerRuntime.parseConfigurationString(WRTIER_ENDPOINT);
        writerRuntime.setTransactionLogging(true).connect();

        dstDataRuntime = CorfuRuntime.fromParameters(params);
        dstDataRuntime.parseConfigurationString(WRTIER_ENDPOINT);
        dstDataRuntime.setTransactionLogging(true).connect();

        dstTestRuntime = CorfuRuntime.fromParameters(params);
        dstTestRuntime.parseConfigurationString(WRTIER_ENDPOINT);
        dstTestRuntime.setTransactionLogging(true).connect();
    }

    public static LogReplicationEntry deserializeTest(LogReplicationEntry entry) {
        LogReplicationEntry newEntry = LogReplicationEntry.deserialize((new DataMessage(entry.serialize()).getData()));
        return newEntry;
    }

    public static void openStreams(HashMap<String, CorfuTable<Long, Long>> tables, CorfuRuntime rt) {
        for (int i = 0; i < NUM_STREAMS; i++) {
            String name = "test" + Integer.toString(i);

            CorfuTable<Long, Long> table = rt.getObjectsView()
                    .build()
                    .setStreamName(name)
                    .setTypeToken(new TypeToken<CorfuTable<Long, Long>>() {
                    })
                    .setSerializer(Serializers.PRIMITIVE)
                    .open();
            tables.put(name, table);
        }
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
        for (int i = 0; i < numT; i++) {
            rt.getObjectsView().TXBegin();
            for (String name : tables.keySet()) {
                hashMap.putIfAbsent(name, new HashMap<>());
                long key = i + startval;
                tables.get(name).put(key, key);
                log.trace("tail " + rt.getAddressSpaceView().getLogTail() + " seq " + rt.getSequencerView().query().getSequence());
                hashMap.get(name).put(key, key);
            }
            rt.getObjectsView().TXEnd();
        }
        System.out.println("generate transactions num " + numT);
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

    public static void verifyNoData(HashMap<String, CorfuTable<Long, Long>> tables) {
        for (CorfuTable table : tables.values()) {
            assertThat(table.keySet().isEmpty()).isTrue();
        }
    }

    /**
     * enforce checkpoint entries at the streams.
     */
    public static void ckStreams(CorfuRuntime rt, HashMap<String, CorfuTable<Long, Long>> tables) {
        MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
        for (CorfuTable map : tables.values()) {
            mcw1.addMap(map);
        }

        Token checkpointAddress = mcw1.appendCheckpoints(rt, "test");

        // Trim the log
        rt.getAddressSpaceView().prefixTrim(checkpointAddress);
        rt.getAddressSpaceView().gc();
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

    public static void readLogEntryMsgs(List<LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsLogEntryReader reader = new StreamsLogEntryReader(rt, config);

        reader.setGlobalBaseSnapshot(Address.NON_ADDRESS, Address.NON_ADDRESS);

        for (int i = 0; i < NUM_TRANSACTIONS; i++) {
            LogReplicationEntry message = reader.read(UUID.randomUUID());
            DataMessage dataMessage = new DataMessage(message.serialize());
            if (dataMessage == null) {
                System.out.println("**********data message is null");
                assertThat(false);
            }
            System.out.println("generate the message " + i);

            msgQ.add(deserializeTest(message));
        }
    }

    public static void writeLogEntryMsgs(List<LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());

        LogEntryWriter writer = new LogEntryWriter(rt, config);

        if (msgQ.isEmpty()) {
            System.out.println("msgQ is empty");
        }

        LogReplicationEntry data = msgQ.get(0);
        //writer.setTimestamp(data.metadata.getSnapshotTimestamp(), Address.NON_ADDRESS);

        for (LogReplicationEntry msg : msgQ) {
            writer.apply(msg);
        }
    }

    public static void clearTables(HashMap<String, CorfuTable<Long, Long>> tables) {
        for (CorfuTable<Long, Long> table : tables.values()) {
            table.clear();
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

    public static void readSnapLogMsgs(List<LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsSnapshotReader reader = new StreamsSnapshotReader(rt, config);

        reader.reset(rt.getAddressSpaceView().getLogTail());
        while (true) {
            SnapshotReadMessage snapshotReadMessage = reader.read(UUID.randomUUID());
            for (LogReplicationEntry data : snapshotReadMessage.getMessages()) {
                msgQ.add(deserializeTest(data));
            }

            if (snapshotReadMessage.isEndRead()) {
                break;
            }
        }
    }

    public static void writeSnapLogMsgs(List<LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, UUID.randomUUID());
        StreamsSnapshotWriter writer = new StreamsSnapshotWriter(rt, config);

        if (msgQ.isEmpty()) {
            System.out.println("msgQ is empty");
        }
        writer.reset(msgQ.get(0).metadata.getSnapshotTimestamp());

        for (LogReplicationEntry msg : msgQ) {
            writer.apply(msg);
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
            PersistedWriterMetadata meta = new PersistedWriterMetadata(writerRuntime, UUID.randomUUID());
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
        openStreams(dstTables, dstDataRuntime);
        verifyData("after snap write at dst", dstTables, srcHashMap);
        System.out.println("test done");
    }


    @Test
    public void testLogEntryTransfer() throws IOException {
        // setup environment
        System.out.println("\ntest start ok");
        setupEnv();

        openStreams(srcTables, srcDataRuntime);
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
        assertThat(diff).isEqualTo(dstEntries);
        printTails("after writing to server2", srcDataRuntime, dstDataRuntime);

        //verify data with hashtable
        openStreams(dstTables, dstDataRuntime);
        verifyData("after log writing at dst", dstTables, srcHashMap);
        System.out.println("test done");
    }
}
