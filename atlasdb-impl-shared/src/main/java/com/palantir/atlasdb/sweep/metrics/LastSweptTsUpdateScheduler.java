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

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public final class LastSweptTsUpdateScheduler implements AutoCloseable {
    private static final SafeLogger log = SafeLoggerFactory.get(LastSweptTsUpdateScheduler.class);
    private static final long DELAY = Duration.ofSeconds(30).toMillis();
    private final ScheduledExecutorService executorService;
    private final Runnable task;

    private LastSweptTsUpdateScheduler(ScheduledExecutorService executorService, Runnable task) {
        this.executorService = executorService;
        this.task = task;
    }

    public static LastSweptTsUpdateScheduler createStarted(Runnable task) {
        ScheduledExecutorService executorService = PTExecutors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("Last Swept Timestamp Metric Update", true));
        LastSweptTsUpdateScheduler scheduler = new LastSweptTsUpdateScheduler(executorService, task);
        scheduler.start();
        return scheduler;
    }

    private void runOnce() {
        try {
            task.run();
        } catch (Exception exception) {
            log.info("Last Swept Timestamp Scheduler task failed ", exception);
        }
    }

    private void start() {
        executorService.scheduleWithFixedDelay(this::runOnce, DELAY, DELAY, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        executorService.shutdown();
    }
}
