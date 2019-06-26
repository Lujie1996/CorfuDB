package org.corfudb.runtime.view.stream;

import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.Iterables;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.StreamOptions;
import org.roaringbitmap.longlong.Roaring64NavigableMap;


/** A view of a stream implemented with address maps.
 *
 * A stream's address space is discovered by requesting to the
 * sequencer, the map of all addresses belonging to this stream.
 *
 * Notice that, unlike the BackpointerStreamView, addresses are discovered
 * in a single call to the sequencer, therefore, entries are not read
 * in advance. For this reason, when iterating over the stream (getNextEntry)
 * we perform batch reads, which significantly reduces the number of RPCs to
 * Log Units, as opposed to reading one entry at a time. Also, single stepping
 * in the presence of holes is no longer a need as address space is determined
 * by the map provided by the sequencer.
 *
 * Created by annym on 04/25/19.
 */
@Slf4j
public class AddressMapStreamView extends AbstractQueuedStreamView {

    private static final int DIFF_CONSECUTIVE_ADDRESSES = 1;

    private long addressCount = 0L;

    /** Create a new address map stream view.
     *
     * @param runtime   The runtime to use for accessing the log.
     * @param streamId  The ID of the stream to view.
     */
    public AddressMapStreamView(final CorfuRuntime runtime,
                                final UUID streamId,
                                @Nonnull final StreamOptions options) {
        super(runtime, streamId, options);
    }

    public AddressMapStreamView(final CorfuRuntime runtime,
                                final UUID streamId) {
        this(runtime, streamId, StreamOptions.DEFAULT);
    }

    @Override
    protected ILogData removeFromQueue(NavigableSet<Long> queue) {
        // Because the discovery mechanism implemented by this class
        // does not require to read log entries in advance (requests
        // full address map in a single call), entries can be read in
        // batches whenever we have a cache miss. This allows next reads
        // to be serviced immediately, rather than reading one entry at a time.
        Long thisRead = queue.pollFirst();

        if (thisRead != null) {
            // In the case that the immediately requested address (thisRead) is not available,
            // read it along with a batch of subsequent addresses.

            ILogData ld;

            try {
                // The read range has a special behaviour, it attempts to read the address of interest,
                // and in the case it is not in the cache, it reads a batch of addresses
                // that will be further required, returning only the data for the address of interest.

                // Because addresses are requested in increasing order and system relies on prefix trim,
                // we force trimmedExceptions to be thrown by lower layers, and handle ignoreTrimmed at this layer.
                // Note that lower layers will cache the valid entries, to optimize read performance.
                ld = read(thisRead, queue);
            } catch (TrimmedException te) {
                if (!options.ignoreTrimmed) {
                    throw te;
                }

                log.warn("removeFromQueue[{}]: ignoring trimmed addresses {}", this, te.getTrimmedAddresses());
                // Ignore trimmed address, remove trimmed addresses and get next from queue
                te.getTrimmedAddresses().forEach(address -> queue.remove(address));
                return removeFromQueue(queue);
            }

            if (queue == getCurrentContext().readQueue && ld != null) {
                // Validate that the data entry belongs to this stream, otherwise, skip.
                // This verification protects from sequencer regression (tokens assigned in an older epoch
                // that were never written to, and reassigned on a newer epoch)
                if (ld.containsStream(this.id)) {
                    addToResolvedQueue(getCurrentContext(), thisRead, ld);
                    return ld;
                } else {
                    log.trace("getNextEntry[{}]: the data for address {} does not belong to this stream. Skip.",
                            this, thisRead);
                    return removeFromQueue(queue);
                }
            }

            return ld;
        }

        return null;
    }

    /**
     * Retrieve this stream's address map, i.e., a map of all addresses corresponding to this stream between
     * (stop address, start address] and return a boolean indicating if addresses were found in this range.
     *
     * @param streamId stream's ID, it is required because this same method can be used for
     *                 discovering addresses from the checkpoint stream.
     * @param queue    queue to insert discovered addresses in the given range
     * @param startAddress range start address (inclusive)
     * @param stopAddress  range end address (exclusive and lower than start address)
     * @param filter       function to filter entries
     * @param checkpoint   boolean indicating if it is a checkpoint stream
     * @param maxGlobal    maximum Address until which we want to sync (is not necessarily equal to start address)
     * @return
     */
    @Override
    protected boolean discoverAddressSpace(final UUID streamId,
                                           final NavigableSet<Long> queue,
                                           final long startAddress,
                                           final long stopAddress,
                                           final Function<ILogData, Boolean> filter,
                                           final boolean checkpoint,
                                           final long maxGlobal) {
        // Sanity check: startAddress must be a valid address and greater than stopAddress.
        // The startAddress refers to the max resolution we want to resolve this stream to,
        // while stopAddress refers to the lower boundary (locally resolved).
        // If startAddress is equal to stopAddress there is nothing to resolve.
        if (Address.isAddress(startAddress) && (startAddress > stopAddress)) {

            StreamAddressSpace streamAddressSpace = getStreamAddressMap(startAddress, stopAddress, streamId);

            if (checkpoint) {
                processCheckpoint(streamAddressSpace, filter, queue);
            } else {
                // Transfer discovered addresses to queue. We must limit to maxGlobal,
                // as startAddress could be ahead of maxGlobal---in case it reflects
                // the tail of the stream.
                queue.addAll(streamAddressSpace.copyAddressesToSet(maxGlobal));

                long trimMark = streamAddressSpace.getTrimMark();
                // Address maps might have been trimmed, hence not reflecting all updates to the stream
                // For this reason, in the case of a valid trim mark, we must be sure this space is
                // already resolved or loaded by a checkpoint.
                if (Address.isAddress(trimMark) && !isTrimCoveredByCheckpointOrLocalView(trimMark)) {
                    String message = String.format("getStreamAddressMap[{%s}] stream has been " +
                                    "trimmed at address %s and this space is not covered by the " +
                                    "loaded checkpoint with start address %s, while accessing the " +
                                    "stream at version %s", this, streamAddressSpace.getTrimMark(),
                            getCurrentContext().checkpoint.checkpointSuccessStartAddr, maxGlobal);
                    log.warn(message);
                    if (options.ignoreTrimmed) {
                        log.warn("getStreamAddressMap[{}]: Ignoring trimmed exception for address[{}].",
                                this, streamAddressSpace.getTrimMark());
                    } else {
                        throw new TrimmedException(message);
                    }
                }
            }
        }

        addressCount += queue.size();

        return !queue.isEmpty();
    }

    private void processCheckpoint(StreamAddressSpace streamAddressSpace, Function<ILogData, Boolean> filter,
                                   NavigableSet<Long> queue) {
        SortedSet<Long> checkpointAddresses = new TreeSet<>(Collections.reverseOrder());
        streamAddressSpace.getAddressMap().forEach(checkpointAddresses::add);

        // Checkpoint entries will be read in batches of a predefined size,
        // the reason not to read them all in a single call is that:
        // 1. There might be more than one checkpoint and not all of them might be required.
        // The latest checkpoint subsumes all other checkpoints (we could be reading
        // more data than necessary).
        // 2. Because trims are async (between sequencer and log unit), part of these
        // addresses (for instance for the first checkpoint) might have been already
        // trimmed from the log, but stream maps (sequencer) still do not reflect it
        // (causing unnecessary TrimmedExceptions--as this checkpoint is not even
        // needed in the first place).
        Iterable<List<Long>> batches = Iterables.partition(checkpointAddresses,
                runtime.getParameters().getCheckpointReadBatchSize());

        boolean checkpointResolved = false;
        for (List<Long> batch : batches) {
            if (checkpointResolved) {
                // No need to fetch next batch of addresses, checkpoint has been resolved.
                break;
            }

            try {
                List<ILogData> entries = readAll(batch);
                for (ILogData data : entries) {
                    filter.apply(data);
                }
            } catch (TrimmedException te) {
                // Read one entry at a time for the last failed batch, this way we might load
                // the required checkpoint entries until the stop condition is fulfilled
                // without hitting a trimmed position. If we do hit a trim it is an actual
                // TrimmedException
                checkpointResolved = processCheckpointBatchByEntry(batch, filter, queue);
            }
        }

        // Select correct checkpoint - Highest
        List<Long> checkpointEntries = selectCheckpoint(getCurrentContext());
        queue.addAll(checkpointEntries);
    }

    /**
     * Process a batch of addresses as single entries.
     *
     * @param batch list of addresses to read.
     * @param filter filter to apply to checkpoint data.
     * @param queue read queue.
     *
     * @return True, resolved checkpoint (reached end of valid checkpoint).
     *         False, otherwise.
     */
    private boolean processCheckpointBatchByEntry(List<Long> batch,
                                                  Function<ILogData, Boolean> filter,
                                                  NavigableSet<Long> queue) {
        long lastReadAddress = Address.NON_ADDRESS;
        try {
            boolean checkpointResolved;
            for (long address : batch) {
                lastReadAddress = address;
                ILogData data = read(address);
                checkpointResolved = filter.apply(data);
                if (checkpointResolved) {
                    // Return if checkpoint has already been resolved (reached stop).
                    return true;
                }
            }
        } catch (TrimmedException ste) {
            // The valid checkpoint has been trimmed.
            if (options.ignoreTrimmed) {
                log.warn("processCheckpoint[{}]: Ignoring trimmed exception for address[{}]," +
                        " stream[{}]", this, lastReadAddress, id);
            } else {
                throw ste;
            }
        }

        return false;
    }

    private StreamAddressSpace getStreamAddressMap(long startAddress, long stopAddress, UUID streamId) {
        // Case non-consecutive addresses
        if (startAddress - stopAddress > DIFF_CONSECUTIVE_ADDRESSES) {
            log.trace("getStreamAddressMap[{}] get addresses from {} to {}", this, startAddress, stopAddress);
            // This step is an optimization.
            // Attempt to read the last entry and verify its backpointer,
            // if this address is already resolved locally do not request the stream map to the sequencer as new
            // updates are not required to be synced (this benefits single runtime writers).
            if(isAddressToBackpointerResolved(startAddress, streamId)) {
                return new StreamAddressSpace(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf(startAddress));
            }

            log.trace("getStreamAddressMap[{}]: request stream address space between {} and {}.",
                        streamId, startAddress, stopAddress);
            return runtime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, startAddress, stopAddress));
        }

        // Start and stop address are consecutive addresses, no need to request the address map for this stream,
        // the only address to include is startAddress (stopAddress is already resolved - not included in the lookup).
        return new StreamAddressSpace(Address.NON_ADDRESS, Roaring64NavigableMap.bitmapOf(startAddress));
    }

    private boolean isAddressToBackpointerResolved(long startAddress, UUID streamId) {
        ILogData d;
        try {
            d = read(startAddress);
        } catch (TrimmedException e) {
            if (options.ignoreTrimmed) {
                log.warn("getStreamAddressMap[{}]: Ignoring trimmed exception for address[{}]," +
                        " stream[{}]", this, startAddress, streamId);
                return false;
            } else {
                throw e;
            }
        }

        if (!runtime.getParameters().isBackpointersDisabled() && d.hasBackpointer(streamId)) {
            long previousAddress = d.getBackpointer(streamId);
            log.trace("getStreamAddressMap[{}]: backpointer for {} points to {}",
                    streamId, startAddress, previousAddress);
            // if backpointer is a valid log address or Address.NON_EXIST (beginning of the stream)
            if ((Address.isAddress(previousAddress) &&
                    getCurrentContext().resolvedQueue.contains(previousAddress))
                    || previousAddress == Address.NON_EXIST) {
                log.trace("getStreamAddressMap[{}]: backpointer {} is locally present, do not request " +
                        "stream address map.", streamId, previousAddress);
                return true;
            }
        }

        return false;
    }

    /**
     * Verify that a trim is covered either by a loaded checkpoint or by the locally resolved addresses.
     *
     * Because address maps might have been trimmed, the trim mark is a 'marker' of addresses that were
     * removed from the map (historical) and that should be covered by a checkpoint.
     *
     * @param trimMark
     * @return TRUE, trim mark contained in checkpoint, FALSE, otherwise.
     */
    private boolean isTrimCoveredByCheckpointOrLocalView(long trimMark) {
        return isTrimResolvedLocally(trimMark) ||
                isTrimCoveredByCheckpoint(trimMark);
    }

    private boolean isTrimResolvedLocally(long trimMark) {
        return getCurrentContext().checkpoint.checkpointSuccessId == null
                && getCurrentContext().resolvedQueue.contains(trimMark);
    }

    private boolean isTrimCoveredByCheckpoint(long trimMark) {
        return getCurrentContext().checkpoint.checkpointSuccessId != null &&
                getCurrentContext().checkpoint.checkpointSuccessStartAddr >= trimMark;
    }

    @Override
    public long getTotalUpdates() {
        return addressCount;
    }
}

