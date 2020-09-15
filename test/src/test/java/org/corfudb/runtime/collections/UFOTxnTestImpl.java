package org.corfudb.runtime.collections;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Thin shim layer around CorfuStore's TxnContext for providing metadata management
 * capabilities.
 *
 * Created by hisundar on 2020-09-16
 */
class UFOTxnTestImpl {
    /**
     * Internal CorfuStore's txnContext
     */
    private TxnContext txnContext;

    public UFOTxnTestImpl(TxnContext txnContext) {
        this.txnContext = txnContext;
    }

    /**
     *************************** WRITE APIs *****************************************
     */

    /**
     * put the value on the specified key create record if it does not exist.
     *
     * @param table    Table object to perform the create/update on.
     * @param key      Key of the record.
     * @param value    Value or payload of the record.
     * @param metadata Metadata associated with the record.
     * @param <K>      Type of Key.
     * @param <V>      Type of Value.
     * @param <M>      Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    UFOTxnTestImpl putRecord(@Nonnull Table<K, V, M> table,
                             @Nonnull final K key,
                             @Nonnull final V value,
                             @Nullable final M metadata) {
        this.txnContext = txnContext.put(table, key, value, metadata);
        return this;
    }

    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    UFOTxnTestImpl putRecord(@Nonnull String tableName,
                             @Nonnull final K key,
                             @Nonnull final V value,
                             @Nullable final M metadata) {
        return this.putRecord(this.txnContext.getTable(tableName), key, value, metadata);
    }

    /**
     * put the value on the specified key create record if it does not exist.
     *
     * @param table    Table object to perform the create/update on.
     * @param key      Key of the record.
     * @param value    Value or payload of the record.
     * @param metadata Metadata associated with the record.
     * @param <K>      Type of Key.
     * @param <V>      Type of Value.
     * @param <M>      Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    UFOTxnTestImpl putRecord(@Nonnull Table<K, V, M> table,
                             @Nonnull final K key,
                             @Nonnull final V value,
                             @Nullable final M metadata,
                             boolean forceSetMetadata) {
        this.txnContext = txnContext.put(table, key, value, metadata);
        return this;
    }

    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    UFOTxnTestImpl putRecord(@Nonnull String tableName,
                             @Nonnull final K key,
                             @Nonnull final V value,
                             @Nullable final M metadata,
                             boolean forceSetMetadata) {
        return this.putRecord(this.txnContext.getTable(tableName), key, value, metadata);
    }
    /**
     * Merges the delta value with the old value by applying a caller specified BiFunction and writes
     * the final value.
     *
     * @param table         Table object to perform the merge operation on.
     * @param key           Key
     * @param mergeOperator Function to apply to get the new value
     * @param recordDelta   Argument to pass to the mutation function
     * @param <K>           Type of Key.
     * @param <V>           Type of Value.
     * @param <M>           Type of Metadata.
     * @return TxnContext instance
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    UFOTxnTestImpl merge(@Nonnull Table<K, V, M> table,
                         @Nonnull final K key,
                         @Nonnull BiFunction<CorfuRecord<V, M>, CorfuRecord<V, M>, CorfuRecord<V, M>> mergeOperator,
                         @Nonnull final CorfuRecord<V, M> recordDelta) {
        this.txnContext = txnContext.merge(table, key, mergeOperator, recordDelta);
        return this;
    }

    /**
     * Clears the entire table.
     *
     * @param table Table object to perform the delete on.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    UFOTxnTestImpl clear(@Nonnull Table<K, V, M> table) {
        this.txnContext = txnContext.clear(table);
        return this;
    }

    /**
     * Deletes the specified key.
     *
     * @param table Table object to perform the delete on.
     * @param key   Key of the record to be deleted.
     * @param <K>   Type of Key.
     * @param <V>   Type of Value.
     * @param <M>   Type of Metadata.
     * @return TxnContext instance.
     */
    @Nonnull
    public <K extends Message, V extends Message, M extends Message>
    UFOTxnTestImpl delete(@Nonnull Table<K, V, M> table,
                          @Nonnull final K key) {
        this.txnContext = txnContext.delete(table, key);
        return this;
    }

    public <K extends Message, V extends Message, M extends Message>
    UFOTxnTestImpl delete(@Nonnull String tableName,
                          @Nonnull final K key) {
        return this.delete(txnContext.getTable(tableName), key);
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
        return txnContext.getRecord(table, key);
    }

    /**
     * Query by a secondary index.
     *
     * @param table     Table object.
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
        return txnContext.getByIndex(table, indexName, indexKey);
    }

    /**
     * Gets the count of records in the table at a particular timestamp.
     *
     * @param table - the table whose count is requested.
     * @return Count of records.
     */
    public <K extends Message, V extends Message, M extends Message>
    int count(@Nonnull final Table<K, V, M> table) {
        return txnContext.count(table);
    }

    /**
     * Gets all the keys of a table.
     *
     * @param table - the table whose keys are requested.
     * @return keyset of the table
     */
    public <K extends Message, V extends Message, M extends Message>
    Set<K> keySet(@Nonnull final Table<K, V, M> table) {
        return txnContext.keySet(table);
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
        return txnContext.executeQuery(table, entryPredicate);
    }

    /**
     * Commit the transaction.
     * The commit call begins a Corfu transaction at the specified snapshot or at the latest snapshot
     * if no snapshot is specified, applies all the updates and then ends the Corfu transaction.
     * The commit returns successfully if the transaction was committed.
     * Otherwise this throws a TransactionAbortedException.
     *
     * @return - address at which the commit of this transaction occurred.
     */
    public long commit() {
        return txnContext.commit();
    }

    /**
     * Explicitly end a read only transaction to clean up resources
     * Explicitly abort a transaction in case of an external condition
     */
    public long endTxn() {
        return txnContext.endTxn();
    }
}
