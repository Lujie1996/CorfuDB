package org.corfudb.runtime.concurrent;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuQueue;
import org.corfudb.runtime.collections.CorfuQueue.CorfuRecordId;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by hisundar on 5/30/19.
 */
@Slf4j
public class CorfuQueueTxTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }

    protected final int numIterations = PARAMETERS.NUM_ITERATIONS_LOW;
    protected final Long numConflictKeys = 2L;

    /**
     * This concurrent test validates that the CorfuQueue::enqueue operations
     * are ordered by that of the parent transaction and fail if transaction aborts.
     *
     * @throws Exception
     */
    @Test
    public void queueOrderedByTransaction() throws Exception {
        final int numThreads = PARAMETERS.CONCURRENCY_TWO;
        Map<Long, Long> conflictMap = instantiateCorfuObject(SMRMap.class, "conflictMap");
        CorfuQueue<String>
                corfuQueue = new CorfuQueue<>(getRuntime(), "testQueue");
        class Record {
            @Getter
            public CorfuRecordId id;
            @Getter
            public String data;

            public Record(CorfuRecordId id, String data) {
                this.id = id;
                this.data = data;
            }
        }
        ArrayList<Record> validator = new ArrayList<>(numThreads * numIterations);
        ReentrantLock lock = new ReentrantLock();

        scheduleConcurrently(numThreads, t ->
        {
            for (Long i = 0L; i < numIterations; i++) {
                String queueData = t.toString() + ":" + i.toString();
                try {
                    TXBegin();
                    Long coinToss = new Random().nextLong() % numConflictKeys;
                    conflictMap.put(coinToss, coinToss);
                    CorfuRecordId id = corfuQueue.enqueue(queueData);
                    lock.lock();
                    final long streamOffset = TXEnd();
                    validator.add(new Record(id, queueData));
                    log.debug("ENQ:" + id + "=>" + queueData + " at " + streamOffset);
                } catch (TransactionAbortedException txException) {
                    log.debug(queueData + " ---> Abort!!! ");
                    // Half the transactions are expected to abort
                } finally {
                    lock.unlock();
                }
            }
        });
        executeScheduled(numThreads, PARAMETERS.TIMEOUT_LONG);

        // After all concurrent transactions are complete, validate that number of Queue entries
        // are the same as the number of successful transactions.
        List<CorfuQueue.CorfuQueueRecord<String>> records = corfuQueue.entryList();
        assertThat(validator.size()).isEqualTo(records.size());

        // Also validate that the order of the queue matches that of the commit order.
        for (int i = 0; i < validator.size(); i++) {
            log.debug("Entry:" + records.get(i).getRecordId());
            assertThat(validator.get(i).getId().equals(records.get(i).getRecordId()));
            assertThat(validator.get(i).getData()).isEqualTo(records.get(i).getEntry());
        }

        // Validate that one cannot compare ID from enqueue with ID from entryList()
        assertThatThrownBy(() -> validator.get(0).getId().compareTo(records.get(0).getRecordId())).
                isExactlyInstanceOf(IllegalArgumentException.class);
    }
}

