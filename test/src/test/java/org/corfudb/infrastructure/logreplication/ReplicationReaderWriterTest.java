package org.corfudb.infrastructure.logreplication;

import org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsSnapshotReader;
import org.corfudb.integration.ReplicationReaderWriterIT;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsLogEntryReader;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.Query;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.StreamOptions;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.runtime.view.stream.OpaqueStream;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.corfudb.integration.ReplicationReaderWriterIT.generateTransactions;
import static org.corfudb.integration.ReplicationReaderWriterIT.openStreams;
import static org.corfudb.integration.ReplicationReaderWriterIT.printTails;
import static org.corfudb.integration.ReplicationReaderWriterIT.verifyData;
import static org.corfudb.integration.ReplicationReaderWriterIT.verifyNoData;

public class ReplicationReaderWriterTest extends AbstractViewTest {
    static public final String PRIMARY_SITE_ID = "Cluster-Paris";

    static private final int START_VAL = 1;
    static final int NUM_KEYS = 4;

    // Enforce to read each entry for each message
    // each log entry size is 62, there are 2 log entry per dataMsg
    // each snapshot entry is 33, there are 4 snapshot entry per dataMsg
    static public final int MAX_MSG_SIZE = 160;
    static private final int snapshotEntryPerMsg = MAX_MSG_SIZE / 33;
    static private final int txEntryPerMsg = MAX_MSG_SIZE / 62;

    static public final int BATCH_SIZE = 2;

    static private final int NUM_TRANSACTIONS = 100;

    CorfuRuntime srcDataRuntime = null;
    CorfuRuntime dstDataRuntime = null;
    CorfuRuntime readerRuntime = null;
    CorfuRuntime writerRuntime = null;

    HashMap<String, CorfuTable<Long, Long>> srcTables = new HashMap<>();
    HashMap<String, CorfuTable<Long, Long>> dstTables = new HashMap<>();
    LogEntryReader logEntryReader;
    LogEntryWriter logEntryWriter;

    /*
     * the in-memory data for corfu tables for verification.
     */
    HashMap<String, HashMap<Long, Long>> hashMap = new HashMap<String, HashMap<Long, Long>>();

    /*
     * store message generated by stream snapshot log reader and will play it at the writer side.
     */
    List<LogReplicationEntry> msgQ = new ArrayList<LogReplicationEntry>();

    public void setup() {
        srcDataRuntime = getDefaultRuntime().connect();
        srcDataRuntime = getNewRuntime(getDefaultNode()).setTransactionLogging(true).connect();
        dstDataRuntime = getNewRuntime(getDefaultNode()).setTransactionLogging(true).connect();
        readerRuntime = getNewRuntime(getDefaultNode()).setTransactionLogging(true).connect();
        writerRuntime = getNewRuntime(getDefaultNode()).setTransactionLogging(true).connect();

        UUID uuid = UUID.randomUUID();
        LogReplicationConfig config = new LogReplicationConfig(hashMap.keySet());
        LogReplicationMetadataManager logReplicationMetadataManager = new LogReplicationMetadataManager(readerRuntime, 0, uuid.toString());
        logEntryReader = new StreamsLogEntryReader(readerRuntime, config);
        logEntryWriter = new LogEntryWriter(writerRuntime, config, logReplicationMetadataManager);
    }

    @Test
    public void testLogEntryReplication() {
        setup();

        openStreams(srcTables, srcDataRuntime);
        generateTransactions(srcTables, hashMap, NUM_TRANSACTIONS, srcDataRuntime, START_VAL);
        printTails("after writing data to src tables", srcDataRuntime, dstDataRuntime);

        readLogEntryMsgs(msgQ, srcTables.keySet(), readerRuntime);
        assertThat(msgQ.size()).isEqualTo(NUM_TRANSACTIONS/txEntryPerMsg);

        writeLogEntryMsgs(msgQ, srcTables.keySet(), writerRuntime);
        printTails("after playing message at dst", srcDataRuntime, dstDataRuntime);
        openStreams(dstTables, dstDataRuntime);

        verifyData("after writing log entry at dst", dstTables, hashMap);
    }

    private void readMsgs(List<LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        LogReplicationConfig config = new LogReplicationConfig(streams, BATCH_SIZE, MAX_MSG_SIZE);
        StreamsSnapshotReader reader = new StreamsSnapshotReader(rt, config);

        reader.reset(rt.getAddressSpaceView().getLogTail());
        while (true) {
            SnapshotReadMessage snapshotReadMessage = reader.read(UUID.randomUUID());
            msgQ.addAll(snapshotReadMessage.getMessages());
            if (snapshotReadMessage.isEndRead()) {
                break;
            }
        }
    }

    @Test
    public void testSnapshotReplication() {
        setup();
        openStreams(srcTables, srcDataRuntime);

        generateTransactions(srcTables, hashMap, NUM_TRANSACTIONS, srcDataRuntime, START_VAL);
        printTails("after writing data to src tables", srcDataRuntime, dstDataRuntime);

        readMsgs(msgQ, hashMap.keySet(), readerRuntime);
        assertThat(msgQ.size()).isEqualTo(NUM_TRANSACTIONS*srcTables.size()/snapshotEntryPerMsg);

        //call clear table
        for (String name : srcTables.keySet()) {
            CorfuTable<Long, Long> table = srcTables.get(name);
            table.clear();
        }

        verifyNoData(srcTables);

        ReplicationReaderWriterIT.writeSnapLogMsgs(msgQ, srcTables.keySet(), writerRuntime);

        //verify data with hashtable
        openStreams(dstTables, dstDataRuntime);
        verifyData("after writing log entry at dst", dstTables, hashMap);
    }

    /**
     * Test the TxBuilder logUpdate API work properly.
     * It first populate tableA with some data. Then read tableA with stream API,
     * then apply the smrEntries to tableB with logUpdate API.
     * Verify that tableB contains all the keys that A has.
     *
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    @Test
    public void testUFOWithLogUpdate() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String namespace = "default_namespace";
        String tableAName = "tableA";
        String tableBName = "tableB";
        String tableCName = "tableC";

        //start runtime 1, populate some data for table A, table C
        CorfuRuntime runtime1 = getDefaultRuntime().setTransactionLogging(true).connect();
        CorfuStore corfuStore1 = new CorfuStore(runtime1);

        Table<Uuid, Uuid, Uuid> tableA = corfuStore1.openTable(namespace, tableAName,
                Uuid.class, Uuid.class, Uuid.class, TableOptions.builder().build());


        Table<Uuid, Uuid, Uuid> tableC = corfuStore1.openTable(namespace, tableCName,
                Uuid.class, Uuid.class, Uuid.class, TableOptions.builder().build());

        UUID uuidA = CorfuRuntime.getStreamID(tableA.getFullyQualifiedTableName());

        //update tableA
        for (int i = 0; i < NUM_KEYS; i ++) {
            UUID uuid = UUID.randomUUID();
            Uuid key = Uuid.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();
            corfuStore1.tx(namespace).update(tableAName, key, key, key).commit();
        }

        //start runtime 2, open A, B as a stream and C as an UFO
        CorfuRuntime runtime2 = getNewRuntime(getDefaultNode()).setTransactionLogging(true).connect();
        CorfuStore corfuStore2 = new CorfuStore(runtime2);
        Table<Uuid, Uuid, Uuid> tableC2 = corfuStore2.openTable(namespace, tableCName,
                Uuid.class, Uuid.class, Uuid.class, TableOptions.builder().build());

        StreamOptions options = StreamOptions.builder()
                .ignoreTrimmed(false)
                .cacheEntries(false)
                .build();

        Stream streamA = (new OpaqueStream(runtime2, runtime2.getStreamsView().
                get(uuidA, options))).streamUpTo(runtime2.getAddressSpaceView().getLogTail());

        IStreamView txStream = runtime2.getStreamsView()
                .getUnsafe(ObjectsView.TRANSACTION_STREAM_ID, StreamOptions.builder()
                        .cacheEntries(false)
                        .build());
        long tail = runtime2.getAddressSpaceView().getLogTail();

        Iterator<OpaqueEntry> iterator = streamA.iterator();

        Table<Uuid, Uuid, Uuid> tableB = corfuStore1.openTable(namespace, tableBName,
                Uuid.class, Uuid.class, Uuid.class, TableOptions.builder().build());

        UUID uuidB = CorfuRuntime.getStreamID(tableB.getFullyQualifiedTableName());

        while (iterator.hasNext()) {
            CorfuStoreMetadata.Timestamp timestamp = corfuStore2.getTimestamp();
            TxBuilder txBuilder = corfuStore2.tx(namespace);

            //runtime2.getObjectsView().TXBegin();

            UUID uuid = UUID.randomUUID();
            Uuid key = Uuid.newBuilder()
                    .setMsb(uuid.getMostSignificantBits()).setLsb(uuid.getLeastSignificantBits())
                    .build();
            txBuilder.update(tableCName, key, key, key);
            OpaqueEntry opaqueEntry = iterator.next();
            for( SMREntry smrEntry : opaqueEntry.getEntries().get(uuidA)) {
                    txBuilder.logUpdate(CorfuRuntime.getStreamID(tableB.getFullyQualifiedTableName()), smrEntry);
            }
            txBuilder.commit(timestamp);
        }


        //verify data at B and C with runtime 1
        txStream.seek(tail);
        Iterator<ILogData> iterator1 = txStream.streamUpTo(runtime2.getAddressSpaceView().getLogTail()).iterator();
        while(iterator1.hasNext()) {
            ILogData data = iterator1.next();
            data.getStreams().contains(uuidB);
        }
        System.out.print("\nstreamBTail " + runtime2.getAddressSpaceView().getAllTails().getStreamTails().get(uuidB));


        Query q = corfuStore1.query(namespace);
        Set<Uuid> aSet = q.keySet(tableAName, null);
        Set<Uuid> bSet = q.keySet(tableBName, null);

        System.out.print("\naSet " + aSet + "\n\nbSet " + bSet);
        assertThat(bSet.containsAll(aSet)).isTrue();
        assertThat(aSet.containsAll(bSet)).isTrue();
    }

    public static void readLogEntryMsgs(List<LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) throws
            TrimmedException {
        LogReplicationConfig config = new LogReplicationConfig(streams, BATCH_SIZE, MAX_MSG_SIZE);
        StreamsLogEntryReader reader = new StreamsLogEntryReader(rt, config);
        reader.setGlobalBaseSnapshot(Address.NON_ADDRESS, Address.NON_ADDRESS);

        LogReplicationEntry entry = null;

        do {
            entry = reader.read(UUID.randomUUID());

            if (entry != null) {
                msgQ.add(entry);
            }

            System.out.println(" msgQ size " + msgQ.size());

        } while (entry != null);
    }

    public static void writeLogEntryMsgs(List<LogReplicationEntry> msgQ, Set<String> streams, CorfuRuntime rt) {
        org.corfudb.infrastructure.logreplication.LogReplicationConfig config = new LogReplicationConfig(streams);
        LogReplicationMetadataManager logReplicationMetadataManager = new LogReplicationMetadataManager(rt, 0, PRIMARY_SITE_ID);
        LogEntryWriter writer = new LogEntryWriter(rt, config, logReplicationMetadataManager);

        if (msgQ.isEmpty()) {
            System.out.println("msgQ is empty");
        }

        for (LogReplicationEntry msg : msgQ) {
            writer.apply(msg);
        }
    }

}