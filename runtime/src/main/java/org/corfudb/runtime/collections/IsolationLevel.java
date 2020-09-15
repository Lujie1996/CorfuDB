package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuStoreMetadata;

/**
 * Used to specify which transaction isolation level will be used for the TxnContext transactions.
 * Following isolation levels are supported:
 *  1. SNAPSHOT() - all writes and reads will happen at a specific timestamp
 *      1.a - the default timestamp is implicitly derived on the first read operation
 *            in read-your-write transactions or at the time of commit in write-only transactions.
 *      1.b - the timestamp can also be explicitly provided. All transactions will read from
 *            and be validated against this specified timestamp.
 *
 *  created by hisundar on 2020-09-09
 */
public class IsolationLevel {
    @Getter
    private Token timestamp;
    // Initialize this class using one of the following Isolation types
    private IsolationLevel(Token timestamp) {
        this.timestamp = timestamp;
    }

    public static IsolationLevel snapshot() {
        return new IsolationLevel(Token.UNINITIALIZED);
    }

    public static IsolationLevel snapshot(CorfuStoreMetadata.Timestamp timestamp) {
        return new IsolationLevel(new Token(timestamp.getEpoch(), timestamp.getSequence()));
    }
}
