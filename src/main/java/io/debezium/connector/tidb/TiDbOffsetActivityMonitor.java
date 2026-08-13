/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.tidb;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The full offset state, the TiCDC commit timestamp together with the per topic-partition
 * stream positions, is compared against the value captured when the monitor was last
 * consulted, and when none have moved, a stale result is reported. The stream positions are
 * compared in addition to the commit timestamp so that progress through non data change
 * messages, which advance the stream position without changing the commit timestamp, is
 * not reported as stale.
 * <p>
 * No check is performed until the first stream position has been recorded, so a connector
 * that has not yet consumed its first message is not reported as stale.
 *
 * @author Chris Cranford
 */
public class TiDbOffsetActivityMonitor implements OffsetActivityMonitor<TiDbPartition, TiDbOffsetContext> {

    private final Duration checkInterval;

    private Map<String, ?> previousOffset;

    public TiDbOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(TiDbPartition partition, TiDbOffsetContext offsetContext) {
        final Map<String, ?> offset = offsetContext.getOffset();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if (offsetContext.hasStreamPosition() && Objects.equals(previousOffset, offset)) {
            result = StaleOffsetsResult.stale(
                    ("Offsets at TiCDC commit timestamp %s have not changed in %d milliseconds. " +
                            "This may indicate the database is idle, there are no changes for the captured tables, " +
                            "or that the TiCDC changefeed is paused or failed, or the connector is no longer " +
                            "receiving messages from the TiCDC topics.")
                            .formatted(offsetContext.getCommitTs(), checkInterval.toMillis()));
        }

        // Update tracked stats
        previousOffset = offset;

        return result;
    }

}