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

package com.palantir.atlasdb.sweep.metrics;

import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepQueue;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class LastSweptTimestampUpdater implements AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(LastSweptTimestampUpdater.class);
    private final SweepQueue queue;
    private final TargetedSweepMetrics metrics;
    private final ScheduledExecutorService executorService;
    private boolean isScheduled;

    public LastSweptTimestampUpdater(
            SweepQueue queue, TargetedSweepMetrics metrics, ScheduledExecutorService executorService) {
        this.queue = queue;
        this.metrics = metrics;
        this.executorService = executorService;
        this.isScheduled = false;
    }

    private void updateLastSweptTimestampMetric(SweeperStrategy sweeperStrategy) {
        int shards = queue.getNumShards();

        Set<ShardAndStrategy> shardAndStrategySet = IntStream.range(0, shards)
                .mapToObj(shard -> ShardAndStrategy.of(shard, sweeperStrategy))
                .collect(Collectors.toSet());

        Map<ShardAndStrategy, Long> shardAndStrategyToTimestamp = queue.getLastSweptTimestamps(shardAndStrategySet);

        KeyedStream.stream(shardAndStrategyToTimestamp).forEach(metrics::updateProgressForShard);
    }

    public Optional<ScheduledFuture<?>> schedule(long delayMillis) {
        Preconditions.checkArgument(
                delayMillis > 0, "Last swept timestamp metric update delay must be strictly positive.");

        if (isScheduled) {
            return Optional.empty();
        }

        Optional<ScheduledFuture<?>> optionalFuture = Optional.of(
                executorService.scheduleWithFixedDelay(this::run, delayMillis, delayMillis, TimeUnit.MILLISECONDS));

        isScheduled = true;
        return optionalFuture;
    }

    private void run() {
        run(SweeperStrategy.CONSERVATIVE);
        run(SweeperStrategy.THOROUGH);
    }

    private void run(SweeperStrategy sweeperStrategy) {
        try {
            updateLastSweptTimestampMetric(sweeperStrategy);
            log.info(
                    "Last Swept Timestamp Update Task ran successfully for ",
                    SafeArg.of("sweeperStrategy", sweeperStrategy));
        } catch (Throwable throwable) {
            log.warn(
                    "Last Swept Timestamp Update Task failed for ",
                    SafeArg.of("sweeperStrategy", sweeperStrategy),
                    throwable);
        }
    }

    @Override
    public void close() {
        executorService.shutdown();
        isScheduled = false;
    }
}
