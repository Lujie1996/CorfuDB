package org.corfudb.runtime.view.replication;

import com.google.common.collect.Range;
import io.opentracing.Scope;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.RecoveryException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.RuntimeLayout;
import org.corfudb.util.CFUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;



/**
 * Created by mwei on 4/6/17.
 */
@Slf4j
public class ChainReplicationProtocol extends AbstractReplicationProtocol {

    public ChainReplicationProtocol(IHoleFillPolicy holeFillPolicy) {
        super(holeFillPolicy);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(RuntimeLayout runtimeLayout, ILogData data) throws OverwriteException {
        final long globalAddress = data.getGlobalAddress();
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);

        // To reduce the overhead of serialization, we serialize only the
        // first time we write, saving when we go down the chain.
        try (ILogData.SerializationHandle sh =
                     data.getSerializedForm()) {
            log.trace("Write[{}]: chain head {}/{}", globalAddress, 1, numUnits);
            // In chain replication, we start at the chain head.
            try (Scope writeScope = runtimeLayout.getRuntime().getParameters().getTracer()
                        .buildSpan("writeHead").startActive(true)) {
                CFUtils.getUninterruptibly(
                        runtimeLayout.getLogUnitClient(globalAddress, 0)
                                .write(sh.getSerialized()),
                        OverwriteException.class);
                propagate(runtimeLayout, globalAddress, sh.getSerialized());
            } catch (OverwriteException oe) {
                // Some other wrote here (usually due to hole fill)
                // We need to invoke the recovery protocol, in case
                // the write wasn't driven to completion.
                recover(runtimeLayout, globalAddress);
                throw oe;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ILogData peek(RuntimeLayout runtimeLayout, long globalAddress) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);
        log.trace("Read[{}]: chain {}/{}", globalAddress, numUnits, numUnits);
        // In chain replication, we read from the last unit, though we can optimize if we
        // know where the committed tail is.
        ILogData ret = CFUtils.getUninterruptibly(
                runtimeLayout
                        .getLogUnitClient(globalAddress, numUnits - 1)
                        .read(globalAddress)).getAddresses()
                .getOrDefault(globalAddress, null);
        return ret == null || ret.isEmpty() ? null : ret;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, ILogData> readAll(RuntimeLayout runtimeLayout, List<Long> globalAddresses) {
        Range<Long> range = Range.encloseAll(globalAddresses);
        return readRange(runtimeLayout, range, true);
    }



    @Override
    public Map<Long, ILogData> readRange(RuntimeLayout runtimeLayout, Set<Long> globalAddresses) {
        Range<Long> range = Range.encloseAll(globalAddresses);
        return readRange(runtimeLayout, range, true);
    }

    /**
     * Reads a range of addresses from the Chain of log unit servers. This method optimizes for the time to wait to
     * hole fill in case an empty address is encountered.
     * If the waitForWrite flag is set to true, when an empty address is encountered, it waits for the hole to be
     * filled. All subsequent empty addresses are hole filled directly and the reader does not wait.
     * In case the flag is set to false, none of the reads wait for write completion and the empty address is hole
     * filled right away.
     *
     * @param runtimeLayout Runtime layout.
     * @param range         Range of addresses to read.
     * @param waitForWrite  Flag whether wait for write is required or hole fill directly.
     * @return Map of read addresses.
     */
    public Map<Long, ILogData> readRange(RuntimeLayout runtimeLayout, Range<Long> range, boolean waitForWrite) {
        long startAddress = range.lowerEndpoint();
        long endAddress = range.upperEndpoint();
        int numUnits = runtimeLayout.getLayout().getSegmentLength(startAddress);
        log.trace("readRange[{}-{}]: chain {}/{}", startAddress, endAddress, numUnits, numUnits);

        Map<Long, LogData> logResult = CFUtils.getUninterruptibly(
                runtimeLayout
                        .getLogUnitClient(startAddress, numUnits - 1)
                        .read(range)).getAddresses();

        // In case of holes, use the standard backoff policy for hole fill for
        // the first entry in the list. All subsequent holes in the list can
        // be hole filled without waiting as we have already waited for the first
        // hole.
        boolean wait = !waitForWrite;
        Map<Long, ILogData> returnResult = new TreeMap<>();
        for (Map.Entry<Long, LogData> entry : logResult.entrySet()) {
            long address = entry.getKey();
            ILogData value = entry.getValue();
            if (value == null || value.isEmpty()) {
                if (!wait) {
                    value = read(runtimeLayout, address);
                    wait = true;
                } else {
                    // try to read the value
                    value = CFUtils.getUninterruptibly(runtimeLayout
                            .getLogUnitClient(startAddress, numUnits - 1).read(address))
                            .getAddresses().get(address);
                    // if value is null, fill the hole and get the value.
                    if (value == null) {
                        holeFill(runtimeLayout, address);
                        value = peek(runtimeLayout, address);
                    }
                }
            }

            returnResult.put(entry.getKey(), value);
        }

        return returnResult;
    }

    /**
     * Propagate a write down the chain, ignoring
     * any overwrite errors. It is expected that the
     * write has already successfully completed at
     * the head of the chain.
     *
     * @param runtimeLayout The epoch stamped client containing the layout to use for propagation.
     * @param globalAddress The global address to start
     *                      writing at.
     * @param data          The data to propagate, or NULL,
     *                      if it is to be a hole.
     */
    private void propagate(RuntimeLayout runtimeLayout,
                             long globalAddress,
                             @Nullable ILogData data) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);

        for (int i = 1; i < numUnits; i++) {
            try (Scope writeScope = runtimeLayout.getRuntime().getParameters().getTracer()
                    .buildSpan("chain"+ i).startActive(true)) {
            log.trace("Propagate[{}]: chain {}/{}", Token.of(runtimeLayout.getLayout().getEpoch(),
                    globalAddress),
                    i + 1, numUnits);
            // In chain replication, we write synchronously to every unit
            // in the chain.
                if (data != null) {
                    CFUtils.getUninterruptibly(
                            runtimeLayout.getLogUnitClient(globalAddress, i)
                                    .write(data),
                            OverwriteException.class);
                } else {
                    Token token = new Token(runtimeLayout.getLayout().getEpoch(), globalAddress);
                    CFUtils.getUninterruptibly(runtimeLayout
                            .getLogUnitClient(globalAddress, i)
                            .fillHole(token), OverwriteException.class);
                }
            } catch (OverwriteException oe) {
                log.info("Propagate[{}]: Completed by other writer", globalAddress);
            }
        }
    }

    /** Recover a failed write at the given global address,
     * driving it to completion by invoking the recovery
     * protocol.
     *
     * <p>When this function returns the given globalAddress
     * is guaranteed to contain a committed value.
     *
     * <p>If there was no data previously written at the address,
     * this function will throw a runtime exception. The
     * recovery protocol should -only- be invoked if we
     * previously were overwritten.
     *
     * @param runtimeLayout     The RuntimeLayout to use for the recovery.
     * @param globalAddress     The global address to drive
     *                          the recovery protocol
     *
     */
    private void recover(RuntimeLayout runtimeLayout, long globalAddress) {
        final Layout layout = runtimeLayout.getLayout();
        // In chain replication, we started writing from the head,
        // and propagated down to the tail. To recover, we start
        // reading from the head, which should have the data
        // we are trying to recover
        int numUnits = layout.getSegmentLength(globalAddress);
        log.warn("Recover[{}]: read chain head {}/{}", Token.of(runtimeLayout.getLayout().getEpoch()
                , globalAddress),
                1, numUnits);
        ILogData ld = CFUtils.getUninterruptibly(runtimeLayout
                .getLogUnitClient(globalAddress, 0)
                .read(globalAddress)).getAddresses().getOrDefault(globalAddress, null);
        // If nothing was at the head, this is a bug and we
        // should fail with a runtime exception, as there
        // was nothing to recover - if the head was removed
        // due to a reconfiguration, a network exception
        // would have been thrown and the client should have
        // retried it's operation (in this case of a write,
        // it should have read to determine whether the
        // write was successful or not.
        if (ld == null || ld.isEmpty()) {
            throw new RecoveryException("Failed to read data during recovery at chain head.");
        }
        // now we go down the chain and write, ignoring any overwrite exception we get.
        for (int i = 1; i < numUnits; i++) {
            log.debug("Recover[{}]: write chain {}/{}", layout, i + 1, numUnits);
            // In chain replication, we write synchronously to every unit
            // in the chain.
            try {
                CFUtils.getUninterruptibly(
                        runtimeLayout.getLogUnitClient(globalAddress, i).write(ld),
                        OverwriteException.class);
                // We successfully recovered a write to this member of the chain
                log.debug("Recover[{}]: recovered write at chain {}/{}", layout, i + 1, numUnits);
            } catch (OverwriteException oe) {
                // This member already had this data (in some cases, the write might have
                // been committed to all members, so this is normal).
                log.debug("Recover[{}]: overwritten at chain {}/{}", layout, i + 1, numUnits);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void holeFill(RuntimeLayout runtimeLayout, long globalAddress) {
        int numUnits = runtimeLayout.getLayout().getSegmentLength(globalAddress);
        log.warn("fillHole[{}]: chain head {}/{}", Token.of(runtimeLayout.getLayout().getEpoch(), globalAddress),
                1, numUnits);
        // In chain replication, we write synchronously to every unit in
        // the chain.
        try {
            Token token = new Token(runtimeLayout.getLayout().getEpoch(), globalAddress);
            CFUtils.getUninterruptibly(runtimeLayout
                    .getLogUnitClient(globalAddress, 0)
                    .fillHole(token), OverwriteException.class);
            propagate(runtimeLayout, globalAddress, null);
        } catch (OverwriteException oe) {
            // The hole-fill failed. We must ensure the other writer's
            // value is adopted before returning.
            recover(runtimeLayout, globalAddress);
        }
    }
}
