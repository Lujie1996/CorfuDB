package org.corfudb.runtime.collections;

import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TransactionAlreadyStartedException;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.TableRegistry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * TxnContext is the access layer for binding all the CorfuStore CRUD operations.
 * It can help reduce the footprint of a CorfuStore transaction by only having writes in it.
 * All mutations/writes are aggregated and applied at once a the time of commit() call
 * where a real corfu transaction is started.
 *
 * Created by hisundar, @wenbinzhu, @pankti-m on 2020-09-15
 */
@Slf4j
public class TxnContext {

    private final ObjectsView objectsView;
    private final TableRegistry tableRegistry;
    private final String namespace;
    private final IsolationLevel isolationLevel;
    private final List<Runnable> operations;

    /**
     * Creates a new TxnContext.
     *
     * @param objectsView   ObjectsView from the Corfu client.
     * @param tableRegistry Table Registry.
     * @param namespace     Namespace boundary defined for the transaction.
     */
    @Nonnull
    TxnContext(@Nonnull final ObjectsView objectsView,
               @Nonnull final TableRegistry tableRegistry,
               @Nonnull final String namespace,
               @Nonnull final IsolationLevel isolationLevel) {
            this.objectsView = objectsView;
        this.tableRegistry = tableRegistry;
        this.namespace = namespace;
        this.isolationLevel = isolationLevel;
        this.operations = new ArrayList<>();
        if (TransactionalContext.isInTransaction()) {
            log.error("Cannot start new transaction in this thread without ending previous one");
            throw new TransactionAlreadyStartedException(TransactionalContext.getRootContext().toString());
        }
    }

    public  <K extends Message, V extends Message, M extends Message>
    Table<K, V, M> getTable(@Nonnull final String tableName) {
        return this.tableRegistry.getTable(this.namespace, tableName);
    }

    /**
     *************************** WRITE APIs *****************************************
     */

    /**
     * put the value on the specified key create record if it does not exist.
     *
     * @param table Table object to perform the create/update on.
     * @param key   Key of the record.
     * @param value Value or payload of the record.
     * @param metadata Metadata associated with the record.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContext put(@Nonnull Table<K, V, M> table,
                   @Nonnull final K key,
                   @Nonnull final V value,
                   @Nullable final M metadata) {
        operations.add(() -> {
            table.put(key, value, metadata);
        });
        return this;
    }

    /**
     * Merges the delta value with the old value by applying a caller specified BiFunction and writes
     * the final value.
     *
     * @param table Table object to perform the merge operation on.
     * @param key            Key
     * @param mergeOperator  Function to apply to get the new value
     * @param recordDelta    Argument to pass to the mutation function
     * @param <K>            Type of Key.
     * @param <V>            Type of Value.
     * @param <M>            Type of Metadata.
     * @return TxnContext instance
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContext merge(@Nonnull Table<K, V, M> table,
                     @Nonnull final K key,
                     @Nonnull BiFunction<CorfuRecord<V, M>, CorfuRecord<V,M>, CorfuRecord<V,M>> mergeOperator,
                     @Nonnull final CorfuRecord<V,M> recordDelta) {
        operations.add(() -> {
            CorfuRecord<V,M> oldRecord = table.get(key);
            CorfuRecord<V, M> mergedRecord;
            try {
                mergedRecord = mergeOperator.apply(oldRecord, recordDelta);
            } catch (Throwable throwable) {
                throw new TransactionAbortedException(
                        new TxResolutionInfo(table.getStreamUUID(), this.isolationLevel.getTimestamp()),
                        AbortCause.USER,
                        throwable,
                        TransactionalContext.getCurrentContext());
            }
            table.put(key, mergedRecord.getPayload(), mergedRecord.getMetadata());
        });
        return this;
    }

    /**
     * Apply a Corfu SMREntry directly to a stream. This can be used for replaying the mutations
     * directly into the underlying stream bypassing the object layer entirely.
     * @param streamId
     * @param updateEntry
     * @return
     */
    public TxnContext logUpdate(UUID streamId, SMREntry updateEntry) {
        operations.add(() -> {
            TransactionalContext.getCurrentContext().logUpdate(streamId, updateEntry);
        });
        return this;
    }

    /**
     * Clears the entire table.
     *
     * @param table Table object to perform the delete on.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <M>       Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContext clear(@Nonnull Table<K, V, M> table) {
        operations.add(table::clearAll);
        return this;
    }

    /**
     * Deletes the specified key.
     *
     * @param table Table object to perform the delete on.
     * @param key       Key of the record to be deleted.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <M>       Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    TxnContext delete(@Nonnull Table<K, V, M> table,
                      @Nonnull final K key) {
        operations.add(() -> table.deleteRecord(key));
        return this;
    }

    /**
     *************************** READ API *****************************************
     */

    /**
     * get the full record from the table given a key.
     * If this is invoked on a Read-Your-Writes transaction, it will result in starting a corfu transaction
     * and applying all the updates done so far.
     *
     * @param table Table object to retrieve the record from
     * @param key   Key of the record.
     * @return CorfuStoreEntry<Key, Value, Metadata> instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    CorfuStoreEntry getRecord(@Nonnull Table<K, V, M> table,
                              @Nonnull final K key) {
        txBeginMaybe();
        operations.forEach(Runnable::run); // Apply all pending mutations for reads to see it.
        operations.clear();
        CorfuRecord<V, M> record = table.get(key);
        return new CorfuStoreEntry(key, record.getPayload(), record.getMetadata());
    }

    /**
     * Query by a secondary index.
     *
     * @param table Table object.
     * @param indexName Index name. In case of protobuf-defined secondary index it is the field name.
     * @param indexKey  Key to query.
     * @param <K>       Type of Key.
     * @param <V>       Type of Value.
     * @param <I>       Type of index/secondary key.
     * @return Result of the query.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message, I extends Comparable<I>>
    List<CorfuStoreEntry<K, V, M>> getByIndex(@Nonnull Table<K, V, M> table,
                                              @Nonnull final String indexName,
                                              @Nonnull final I indexKey) {
        txBeginMaybe();
        operations.forEach(Runnable::run); // Apply all pending mutations for reads to see it.
        operations.clear();
        return table.getByIndex(indexName, indexKey);
    }

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param table - the table whose count is requested.
     * @return Count of records.
     */
    public <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull final Table<K, V, M> table) {
        txBeginMaybe();
        operations.forEach(Runnable::run); // Apply all pending mutations for reads to see it.
        operations.clear();
        return table.count();
    }

    /**
     * Gets all the keys of a table.
     *
     * @param table - the table whose keys are requested.
     * @return keyset of the table
     */
    public <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull final Table<K, V, M> table) {
        txBeginMaybe();
        operations.forEach(Runnable::run); // Apply all pending mutations for reads to see it.
        operations.clear();
        return table.keySet();
    }

    /**
     * Scan and filter by entry.
     *
     * @param entryPredicate Predicate to filter the entries.
     * @return Collection of filtered entries.
     */
    public <K extends Message, V extends Message, M extends Message>
    List<CorfuStoreEntry<K, V, M>> executeQuery(@Nonnull final Table<K, V, M> table,
                                                @Nonnull final Predicate<CorfuStoreEntry<K, V, M>> entryPredicate) {
        txBeginMaybe();
        operations.forEach(Runnable::run); // Apply all pending mutations for reads to see it.
        operations.clear();
        return table.scanAndFilterByEntry(entryPredicate);
    }

    /**
     * Start the corfu transaction. When not reading transactional writes, the internal
     * corfu transaction will begin on transaction commit()
     */
    private boolean txBeginMaybe() {
        if (TransactionalContext.isInTransaction()) {
            return true;
        }
        Transaction.TransactionBuilder transactionBuilder = this.objectsView
                .TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE);
        if (isolationLevel.getTimestamp() != Token.UNINITIALIZED) {
            transactionBuilder.snapshot(isolationLevel.getTimestamp());
        }
        transactionBuilder.build().begin();
        return false;
    }

    private long txEndInternal() {
        if (TransactionalContext.isInTransaction()) {
            return this.objectsView.TXEnd();
        }
        return Address.NON_ADDRESS;
    }

    /**
     * Commit the transaction.
     * The commit call begins a Corfu transaction at the specified snapshot or at the latest snapshot
     * if no snapshot is specified, applies all the updates and then ends the Corfu transaction.
     * The commit returns successfully if the transaction was committed.
     * Otherwise this throws a TransactionAbortedException.
     * @return - address at which the commit of this transaction occurred.
     */
    public long commit() {
        long commitAddress;
        try { // batch up all the operations and apply them in a new transaction started here.
            txBeginMaybe();
            operations.forEach(Runnable::run);
        } finally {
            commitAddress = endTxn();
        }
        return commitAddress;
    }

    /**
     * Explicitly end a read only transaction to clean up resources
     * Explicitly abort a transaction in case of an external condition
     */
    public long endTxn() {
        operations.clear();
        return txEndInternal();
    }
}