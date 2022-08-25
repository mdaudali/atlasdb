/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.sweep.queue;

import com.google.common.collect.ImmutableSet;
import java.util.Optional;

public class NonSweepableTimestampsReader {
    private final SweepableTimestamps sweepableTimestamps;
    private final SweepableCells sweepableCells;
    private final ShardProgress progress;

    NonSweepableTimestampsReader(
            SweepableTimestamps sweepableTimestamps, SweepableCells sweepableCells, ShardProgress progress) {
        this.sweepableTimestamps = sweepableTimestamps;
        this.sweepableCells = sweepableCells;
        this.progress = progress;
    }

    public void getNextBatch(long sweepTs) {
        long lastSweptTimestamp = progress.getLastSweptTimestamp(SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE);
        boolean hasNext = true;

        while (hasNext) {
            Optional<Long> nextFinePartition =
                    sweepableTimestamps.nextNonSweepableTimestampPartition(lastSweptTimestamp, sweepTs);
            if (nextFinePartition.isEmpty()) {
                if (SweepQueueUtils.tsPartitionFine(lastSweptTimestamp)
                        < SweepQueueUtils.tsPartitionFine(sweepTs - 1)) {
                    sweepableCells.deleteNonSweepableRows(
                            ImmutableSet.of(SweepQueueUtils.tsPartitionFine(lastSweptTimestamp)));
                }
                if (SweepQueueUtils.tsPartitionCoarse(lastSweptTimestamp)
                        < SweepQueueUtils.tsPartitionCoarse(sweepTs - 1)) {
                    sweepableTimestamps.deleteNonSweepableCoarsePartitions(
                            ImmutableSet.of(SweepQueueUtils.tsPartitionCoarse(lastSweptTimestamp)));
                }
                lastSweptTimestamp = sweepTs - 1;
                progress.updateLastSweptTimestamp(SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE, lastSweptTimestamp);
                return;
            }
            NonSweepableBatchInfo batch = sweepableCells.getNonSweepableBatchForPartition(
                    nextFinePartition.get(), lastSweptTimestamp, sweepTs);
            progress.updateLastSeenCommitTimestamp(
                    SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE, batch.lastSeenCommitTimestamp());
            // todo (gmaretic) : update abandoned timestamps
            lastSweptTimestamp = !batch.processedAll()
                    ? batch.lastSweptTimestamp()
                    : Math.min(sweepTs - 1, SweepQueueUtils.maxTsForFinePartition(nextFinePartition.get()));
            if (lastSweptTimestamp == SweepQueueUtils.maxTsForFinePartition(nextFinePartition.get())) {
                sweepableCells.deleteNonSweepableRows(ImmutableSet.of(nextFinePartition.get()));
            }
            if (SweepQueueUtils.tsPartitionCoarse(lastSweptTimestamp)
                    < SweepQueueUtils.tsPartitionCoarse(batch.lastSweptTimestamp())) {
                sweepableTimestamps.deleteNonSweepableCoarsePartitions(
                        ImmutableSet.of(SweepQueueUtils.tsPartitionCoarse(lastSweptTimestamp)));
            }
            progress.updateLastSweptTimestamp(SweepQueueUtils.DUMMY_SAS_FOR_NON_SWEEPABLE, lastSweptTimestamp);
            hasNext = batch.processedAll() && lastSweptTimestamp < sweepTs - 1;
        }
    }
}
