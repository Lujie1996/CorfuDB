package org.corfudb.protocols.wireprotocol;

import io.netty.buffer.ByteBuf;

import lombok.AllArgsConstructor;
import lombok.Data;

import org.corfudb.runtime.view.Layout;

/**
 * If the first two phases (prepare and propose)  of paxos have been accepted,
 * the proposer sends a Committed message to commit the proposed {@link Layout}.
 *
 * <p>Created by mdhawan on 10/24/16.</p>
 */
@Data
@AllArgsConstructor
public class LayoutCommittedRequest implements ICorfuPayload<LayoutCommittedRequest> {
    private Boolean force;
    private long epoch;
    private Layout layout;

    public LayoutCommittedRequest(long epoch, Layout layout) {
        this.epoch = epoch;
        this.layout = layout;
        this.force = false;
    }

    public LayoutCommittedRequest(ByteBuf buf) {
        force = ICorfuPayload.fromBuffer(buf, Boolean.class);
        epoch = ICorfuPayload.fromBuffer(buf, Long.class);
        layout = ICorfuPayload.fromBuffer(buf, Layout.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, force);
        ICorfuPayload.serialize(buf, epoch);
        ICorfuPayload.serialize(buf, layout);
    }
}
