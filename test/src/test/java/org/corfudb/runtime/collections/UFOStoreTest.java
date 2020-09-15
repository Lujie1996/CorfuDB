package org.corfudb.runtime.collections;

import com.google.common.collect.Iterables;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.test.SampleSchema;
import org.junit.Test;

import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * To ensure that feature changes in CorfuStore do not break verticals,
 * we simulate their usage pattern with a implementation and tests.
 *
 * Created by hisundar on 2020-09-16
 */
@Slf4j
public class UFOStoreTest extends AbstractViewTest {
    /**
     * UFOStoreTestImpl stores 3 pieces of information - key, value and metadata
     * This test demonstrates how metadata field options esp "version" can be used and verified.
     *
     * @throws Exception
     */
    @Test
    public void checkMetadataTransactions() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        UFOStoreTestImpl ufoStore = new UFOStoreTestImpl(corfuRuntime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<SampleSchema.Uuid, SampleSchema.EventInfo, SampleSchema.ManagedResources> table = ufoStore.openTable(
                nsxManager,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.EventInfo.class,
                SampleSchema.ManagedResources.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        SampleSchema.Uuid key1 = SampleSchema.Uuid.newBuilder()
                .setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits())
                .build();
        SampleSchema.ManagedResources user_1 = SampleSchema.ManagedResources.newBuilder().setCreateUser("user_1").build();
        SampleSchema.ManagedResources user_2 = SampleSchema.ManagedResources.newBuilder().setCreateUser("user_2").build();
        long expectedVersion = 0L;

        ufoStore.txn(nsxManager)
                .putRecord(tableName, key1,
                        SampleSchema.EventInfo.newBuilder().setName("abc").build(),
                        user_1)
                .commit();
        assertThat(ufoStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(SampleSchema.ManagedResources.newBuilder()
                        .setCreateUser("user_1")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion++).build());

        // Set the version field to the correct value 1 and expect that no exception is thrown
        ufoStore.txn(nsxManager)
                .putRecord(tableName,
                           key1,
                           SampleSchema.EventInfo.newBuilder().setName("bcd").build(),
                           SampleSchema.ManagedResources.newBuilder()
                                   .setCreateUser("user_2").setVersion(0L).build())
                .commit();
        assertThat(ufoStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(SampleSchema.ManagedResources.newBuilder()
                        .setCreateUser("user_2")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion++).build());

        // Now do an update without setting the version field, and it should not get validated!
        ufoStore.txn(nsxManager)
                .putRecord(tableName,
                           key1,
                           SampleSchema.EventInfo.newBuilder().setName("cde").build(),
                           user_2)
                .commit();
        assertThat(ufoStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(SampleSchema.ManagedResources.newBuilder()
                        .setCreateUser("user_2")
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion).build());

        ufoStore.txn(nsxManager).delete(tableName, key1).commit();
        assertThat(ufoStore.getTable(nsxManager, tableName).get(key1)).isNull();
        expectedVersion = 0L;

        ufoStore.txn(nsxManager)
                .putRecord(tableName,
                           key1,
                           SampleSchema.EventInfo.newBuilder().setName("def").build(),
                           user_2)
                .commit();
        assertThat(ufoStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isEqualTo(SampleSchema.ManagedResources.newBuilder(user_2)
                        .setCreateTimestamp(0L)
                        .setNestedType(SampleSchema.NestedTypeA.newBuilder().build())
                        .setVersion(expectedVersion).build());

        // Validate that if Metadata schema is specified, a null metadata is not acceptable
        assertThatThrownBy(() ->
                table.update(key1, SampleSchema.EventInfo.getDefaultInstance(), null))
                .isExactlyInstanceOf(RuntimeException.class);

        // Validate that we throw a StaleObject exception if there is an explicit version mismatch
        SampleSchema.ManagedResources wrongRevisionMetadata = SampleSchema.ManagedResources.newBuilder(user_2)
                .setVersion(2).build();

        assertThatThrownBy(() ->
                table.update(key1, SampleSchema.EventInfo.getDefaultInstance(), wrongRevisionMetadata))
                .isExactlyInstanceOf(RuntimeException.class);

        // Verify the table is readable using entryStream()
        final int batchSize = 50;
        Stream<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.EventInfo, SampleSchema.ManagedResources>> entryStream = table.entryStream();
        final Iterable<List<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.EventInfo, SampleSchema.ManagedResources>>> partitions =
                Iterables.partition(entryStream::iterator, batchSize);
        for (List<CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.EventInfo, SampleSchema.ManagedResources>> partition : partitions) {
            for (CorfuStoreEntry<SampleSchema.Uuid, SampleSchema.EventInfo, SampleSchema.ManagedResources> entry : partition) {
                assertThat(entry.getKey()).isExactlyInstanceOf(SampleSchema.Uuid.class);
                assertThat(entry.getPayload()).isExactlyInstanceOf(SampleSchema.EventInfo.class);
                assertThat(entry.getMetadata()).isExactlyInstanceOf(SampleSchema.ManagedResources.class);
            }
        }
    }

    /**
     * Demonstrates that opening same table from multiple threads will retry internal transactions
     *
     * @throws Exception
     */
    @Test
    public void checkOpenRetriesTXN() throws Exception {
        CorfuRuntime corfuRuntime = getDefaultRuntime();
        UFOStoreTestImpl ufoStore = new UFOStoreTestImpl(corfuRuntime);
        final String nsxManager = "nsx-manager"; // namespace for the table
        final String tableName = "EventInfo"; // table name
        final int numThreads = 10;
        scheduleConcurrently(numThreads, t -> {
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_MODERATE; i++) {
                // Create & Register the table.
                // This is required to initialize the table for the current corfu client.
                ufoStore.openTable(nsxManager, tableName, SampleSchema.Uuid.class, SampleSchema.EventInfo.class, null,
                        TableOptions.builder().build());
            }

        });
        executeScheduled(numThreads, PARAMETERS.TIMEOUT_LONG);
    }

    /**
     * Demonstrates the checks that the metadata passed into UFOStoreTestImpl is validated against.
     *
     * @throws Exception
     */
    @Test
    public void checkNullMetadataTransactions() throws Exception {

        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        UFOStoreTestImpl ufoStore = new UFOStoreTestImpl(corfuRuntime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<SampleSchema.Uuid, SampleSchema.EventInfo, SampleSchema.ManagedResources> table = ufoStore.openTable(
                nsxManager,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.EventInfo.class,
                null,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        UUID uuid1 = UUID.nameUUIDFromBytes("1".getBytes());
        SampleSchema.Uuid key1 = SampleSchema.Uuid.newBuilder().setMsb(uuid1.getMostSignificantBits()).setLsb(uuid1.getLeastSignificantBits()).build();
        ufoStore.txn(nsxManager)
                .putRecord(tableName,
                        key1,
                        SampleSchema.EventInfo.newBuilder().setName("abc").build(),
                        null)
                .commit();
        assertThat(ufoStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isNull();

        ufoStore.txn(nsxManager)
                .putRecord(tableName,
                           key1,
                           SampleSchema.EventInfo.newBuilder().setName("bcd").build(),
                           SampleSchema.ManagedResources.newBuilder().setCreateUser("testUser").setVersion(1L).build())
                .commit();
        assertThat(ufoStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isNull();

        ufoStore.txn(nsxManager)
                .putRecord(tableName,
                           key1,
                           SampleSchema.EventInfo.newBuilder().setName("cde").build(),
                        null)
                .commit();
        assertThat(ufoStore.getTable(nsxManager, tableName).get(key1).getMetadata())
                .isNull();
    }

    /**
     * Validate that fields of metadata that are not set explicitly retain their prior values.
     *
     * @throws Exception
     */
    @Test
    public void checkMetadataMergesOldFieldsTest() throws Exception {
        // Get a Corfu Runtime instance.
        CorfuRuntime corfuRuntime = getDefaultRuntime();

        // Creating Corfu Store using a connected corfu client.
        UFOStoreTestImpl ufoStore = new UFOStoreTestImpl(corfuRuntime);

        // Define a namespace for the table.
        final String nsxManager = "nsx-manager";
        // Define table name.
        final String tableName = "EventInfo";

        // Create & Register the table.
        // This is required to initialize the table for the current corfu client.
        Table<SampleSchema.Uuid, SampleSchema.EventInfo, SampleSchema.ManagedResources> table = ufoStore.openTable(
                nsxManager,
                tableName,
                SampleSchema.Uuid.class,
                SampleSchema.EventInfo.class,
                SampleSchema.ManagedResources.class,
                // TableOptions includes option to choose - Memory/Disk based corfu table.
                TableOptions.builder().build());

        SampleSchema.Uuid key = SampleSchema.Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
        SampleSchema.EventInfo value = SampleSchema.EventInfo.newBuilder().setName("simpleValue").build();

        long timestamp = System.currentTimeMillis();
        ufoStore.txn(nsxManager)
                .putRecord(tableName, key, value,
                           SampleSchema.ManagedResources.newBuilder()
                                   .setCreateTimestamp(timestamp).build())
                .commit();

        ufoStore.txn(nsxManager)
                .putRecord(tableName, key, value,
                        SampleSchema.ManagedResources.newBuilder().setCreateUser("CreateUser").build())
                .commit();

        CorfuRecord<SampleSchema.EventInfo, SampleSchema.ManagedResources> record1 = table.get(key);
        assertThat(record1.getMetadata().getCreateTimestamp()).isEqualTo(timestamp);
        assertThat(record1.getMetadata().getCreateUser()).isEqualTo("CreateUser");
    }
}
