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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertThrows;

import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.sweep.queue.SweepQueue;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(Parameterized.class)
public class LastSweptTimestampUpdaterTest {

    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Parameterized.Parameters(name = "shards = {0}")
    public static Object[] shards() {
        return new Object[] {1, 8, 16};
    }

    private static final long REFRESH_MILLIS = 10L;
    private static final int TICK_COUNT = 5;
    private static final long CONS_TS = 100L;
    private static final long THOR_TS = 200L;
    private static final ShardAndStrategy CONS_SHARD = ShardAndStrategy.conservative(0);
    private Set<ShardAndStrategy> conservativeShardAndStrategySet;
    private Map<ShardAndStrategy, Long> conservativeShardAndStrategyMap;
    private Set<ShardAndStrategy> thoroughShardAndStrategySet;
    private Map<ShardAndStrategy, Long> thoroughShardAndStrategyMap;
    private final int shards;
    @Mock
    private SweepQueue queue;
    @Mock
    private TargetedSweepMetrics metrics;
    private LastSweptTimestampUpdater lastSweptTimestampUpdater;
    private DeterministicScheduler executorService;

    public LastSweptTimestampUpdaterTest(int shards) {
        this.shards = shards;
    }

    @Before
    public void setUp() {
        executorService = Mockito.spy(new DeterministicScheduler());
        lastSweptTimestampUpdater = new LastSweptTimestampUpdater(queue, metrics, executorService);

        conservativeShardAndStrategySet = buildShardAndStrategySet(SweeperStrategy.CONSERVATIVE);
        conservativeShardAndStrategyMap = buildShardAndStrategyMap(conservativeShardAndStrategySet, CONS_TS);

        thoroughShardAndStrategySet = buildShardAndStrategySet(SweeperStrategy.THOROUGH);
        thoroughShardAndStrategyMap = buildShardAndStrategyMap(thoroughShardAndStrategySet, THOR_TS);
    }

    @Test
    public void unscheduledTaskDoesNotInteractWithExecutorService() {
        Mockito.verifyNoInteractions(executorService);
    }

    @Test
    public void taskThrowsOnInvalidRefreshMillis() {
        assertThrows(SafeIllegalArgumentException.class, () -> lastSweptTimestampUpdater.schedule(0L));
        assertThrows(SafeIllegalArgumentException.class, () -> lastSweptTimestampUpdater.schedule(-REFRESH_MILLIS));
    }

    @Test
    public void scheduleCallSubmitsRunnableToExecutorService() {
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        Mockito.verify(executorService, Mockito.times(1)).scheduleWithFixedDelay(
                Mockito.any(),
                Mockito.eq(REFRESH_MILLIS),
                Mockito.eq(REFRESH_MILLIS),
                Mockito.eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void scheduledTaskDoesNotInteractWithMetricsOrQueueBeforeDelayIsElapsed() {
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        executorService.tick(REFRESH_MILLIS - 1, TimeUnit.MILLISECONDS);
        Mockito.verifyNoMoreInteractions(queue, metrics);
    }

    @Test
    public void firstCallToScheduleReturnsFullOptional() {
        assertThat(lastSweptTimestampUpdater.schedule(REFRESH_MILLIS)).isNotEmpty();
    }

    @Test
    public void secondAndSubsequentCallsToScheduleReturnEmptyOptional() {
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        assertThat(lastSweptTimestampUpdater.schedule(REFRESH_MILLIS)).isEmpty();
        assertThat(lastSweptTimestampUpdater.schedule(REFRESH_MILLIS)).isEmpty();
        assertThat(lastSweptTimestampUpdater.schedule(REFRESH_MILLIS)).isEmpty();
    }

    @Test
    public void scheduledTaskInteractsWithMetricsAndQueueAsExpectedAfterOneDelay() {
        stubWithRealisticReturnValues();
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        executorService.tick(REFRESH_MILLIS, TimeUnit.MILLISECONDS);

        Mockito.verify(queue, Mockito.times(2)).getNumShards();
        Mockito.verify(queue, Mockito.times(1)).getLastSweptTimestamps(conservativeShardAndStrategySet);
        Mockito.verify(queue, Mockito.times(1)).getLastSweptTimestamps(thoroughShardAndStrategySet);

        for (int shard = 0; shard < shards; shard++){
            Mockito.verify(metrics, Mockito.times(1)).updateProgressForShard(ShardAndStrategy.conservative(shard),
                    CONS_TS);
            Mockito.verify(metrics, Mockito.times(1)).updateProgressForShard(ShardAndStrategy.thorough(shard),
                    THOR_TS);
        }
        Mockito.verifyNoMoreInteractions(queue, metrics);
    }

    @Test
    public void scheduledTaskInteractsWithMetricsAndQueueAsExpectedAfterMultipleDelays() {
        stubWithRealisticReturnValues();
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        executorService.tick(TICK_COUNT * REFRESH_MILLIS, TimeUnit.MILLISECONDS);

        Mockito.verify(queue, Mockito.times(2 * TICK_COUNT)).getNumShards();
        Mockito.verify(queue, Mockito.times(TICK_COUNT)).getLastSweptTimestamps(conservativeShardAndStrategySet);
        Mockito.verify(queue, Mockito.times(TICK_COUNT)).getLastSweptTimestamps(thoroughShardAndStrategySet);

        for (int shard = 0; shard < shards; shard++){
            Mockito.verify(metrics, Mockito.times(TICK_COUNT)).updateProgressForShard(ShardAndStrategy.conservative(shard),
                    CONS_TS);
            Mockito.verify(metrics, Mockito.times(TICK_COUNT)).updateProgressForShard(ShardAndStrategy.thorough(shard),
                    THOR_TS);
        }
        Mockito.verifyNoMoreInteractions(queue, metrics);
    }

    @Test
    public void scheduledTaskKeepsRunningAfterUpdateProgressForShardFails() throws ExecutionException,
            InterruptedException {
        Mockito.when(queue.getLastSweptTimestamps(Mockito.anySet()))
                .thenReturn(Collections.singletonMap(CONS_SHARD, CONS_TS));

        Mockito.doThrow(RuntimeException.class)
                .doThrow(RuntimeException.class)
                .doThrow(Error.class)
                .doThrow(Error.class)
                .doNothing()
                .when(metrics)
                .updateProgressForShard(Mockito.any(), Mockito.anyLong());

        ScheduledFuture<?> future = lastSweptTimestampUpdater.schedule(REFRESH_MILLIS).orElseThrow();

        executorService.tick(REFRESH_MILLIS, TimeUnit.MILLISECONDS);
        future.get();

        executorService.tick(REFRESH_MILLIS, TimeUnit.MILLISECONDS);
        future.get();

        executorService.tick(REFRESH_MILLIS, TimeUnit.MILLISECONDS);
        Mockito.verify(metrics, Mockito.times(2 * 3)).updateProgressForShard(CONS_SHARD, CONS_TS);
    }

    @Test
    public void callToCloseOnScheduledTaskCallsExecutorShutdown() {
        Mockito.doNothing().when(executorService).shutdown();
        lastSweptTimestampUpdater.schedule(REFRESH_MILLIS);
        executorService.tick(REFRESH_MILLIS, TimeUnit.MILLISECONDS);
        lastSweptTimestampUpdater.close();
        Mockito.verify(executorService, Mockito.times(1)).shutdown();
    }

    private void stubWithRealisticReturnValues() {
        Mockito.when(queue.getNumShards())
                .thenReturn(shards);
        Mockito.when(queue.getLastSweptTimestamps(conservativeShardAndStrategySet))
                .thenReturn(conservativeShardAndStrategyMap);
        Mockito.when(queue.getLastSweptTimestamps(thoroughShardAndStrategySet))
                .thenReturn(thoroughShardAndStrategyMap);
    }

    private static Map<ShardAndStrategy, Long> buildShardAndStrategyMap(Set<ShardAndStrategy> shardAndStrategySet,
            long timestamp) {
        return KeyedStream.of(shardAndStrategySet)
                .map(_unused -> timestamp)
                .collectToMap();
    }

    private Set<ShardAndStrategy> buildShardAndStrategySet(SweeperStrategy sweeperStrategy) {
        return IntStream.range(0, shards)
                .mapToObj(shard -> ShardAndStrategy.of(shard, sweeperStrategy))
                .collect(Collectors.toSet());
    }

}
