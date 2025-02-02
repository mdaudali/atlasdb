/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.lock.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.common.api.annotations.ReviewedRestrictedApiUsage;
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.common.base.Throwables;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockLeaseRefresher;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.TimestampLeaseResults;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.timestamp.CloseableTimestampService;
import com.palantir.timestamp.RequestBatchingTimestampService;
import com.palantir.timestamp.TimestampRange;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

// Ideally, this class shouldn't be a plain TimelockService, but the amount of effort that would be required to break
// the interface into two distinct components is large.
public class TimeLockClient implements AutoCloseable, TimelockService {
    private static final ScheduledExecutorService refreshExecutor = createSingleThreadScheduledExecutor("refresh");

    private static final long REFRESH_INTERVAL_MILLIS = 5_000;

    private final TimelockService delegate;
    private final CloseableTimestampService timestampService;
    private final LockRefresher<LockToken> lockRefresher;
    private final TimeLockUnlocker unlocker;

    public static TimeLockClient createDefault(TimelockService timelockService) {
        AsyncTimeLockUnlocker asyncUnlocker = AsyncTimeLockUnlocker.create(timelockService);
        RequestBatchingTimestampService timestampService =
                RequestBatchingTimestampService.create(new TimelockServiceErrorDecorator(timelockService));
        return new TimeLockClient(
                timelockService, timestampService, createLockRefresher(timelockService), asyncUnlocker);
    }

    public static TimeLockClient createDefault(TimelockService timelockService, long lockRefreshIntervalMillis) {
        AsyncTimeLockUnlocker asyncUnlocker = AsyncTimeLockUnlocker.create(timelockService);
        RequestBatchingTimestampService timestampService =
                RequestBatchingTimestampService.create(new TimelockServiceErrorDecorator(timelockService));
        return new TimeLockClient(
                timelockService,
                timestampService,
                createLockRefresher(timelockService, lockRefreshIntervalMillis),
                asyncUnlocker);
    }

    public static TimeLockClient withSynchronousUnlocker(TimelockService timelockService) {
        CloseableTimestampService timestampService = new TimelockServiceErrorDecorator(timelockService);
        return new TimeLockClient(
                timelockService, timestampService, createLockRefresher(timelockService), timelockService::unlock);
    }

    @VisibleForTesting
    TimeLockClient(
            TimelockService delegate,
            CloseableTimestampService timestampService,
            LockRefresher<LockToken> lockRefresher,
            TimeLockUnlocker unlocker) {
        this.delegate = delegate;
        this.timestampService = timestampService;
        this.lockRefresher = lockRefresher;
        this.unlocker = unlocker;
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized() && timestampService.isInitialized();
    }

    @Override
    public long getFreshTimestamp() {
        return timestampService.getFreshTimestamp();
    }

    @Override
    public long getCommitTimestamp(long startTs, LockToken commitLocksToken) {
        return delegate.getCommitTimestamp(startTs, commitLocksToken);
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return timestampService.getFreshTimestamps(numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp() {
        LockImmutableTimestampResponse response = executeOnTimeLock(delegate::lockImmutableTimestamp);
        lockRefresher.registerLocks(ImmutableSet.of(response.getLock()));
        return response;
    }

    @Override
    public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        List<StartIdentifiedAtlasDbTransactionResponse> responses =
                executeOnTimeLock(() -> delegate.startIdentifiedAtlasDbTransactionBatch(count));
        Set<LockToken> immutableTsLocks = responses.stream()
                .map(response -> response.immutableTimestamp().getLock())
                .collect(Collectors.toSet());
        try {
            lockRefresher.registerLocks(immutableTsLocks);
        } catch (Throwable t) {
            unlock(immutableTsLocks);
            throw Throwables.throwUncheckedException(t);
        }
        return responses;
    }

    @Override
    public long getImmutableTimestamp() {
        return executeOnTimeLock(delegate::getImmutableTimestamp);
    }

    @Override
    public LockResponse lock(LockRequest request) {
        return lock(request, ClientLockingOptions.getDefault());
    }

    @Override
    public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
        LockResponse response = executeOnTimeLock(() -> delegate.lock(lockRequest));
        if (response.wasSuccessful()) {
            lockRefresher.registerLocks(ImmutableSet.of(response.getToken()), options);
        }
        return response;
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return executeOnTimeLock(() -> delegate.waitForLocks(request));
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return executeOnTimeLock(() -> delegate.refreshLockLeases(tokens));
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        lockRefresher.unregisterLocks(tokens);
        return executeOnTimeLock(() -> delegate.unlock(tokens));
    }

    @Override
    public void tryUnlock(Set<LockToken> tokens) {
        lockRefresher.unregisterLocks(tokens);
        unlocker.enqueue(tokens);
    }

    @Override
    public long currentTimeMillis() {
        return executeOnTimeLock(delegate::currentTimeMillis);
    }

    @ReviewedRestrictedApiUsage
    @Override
    public TimestampLeaseResults acquireTimestampLeases(Map<TimestampLeaseName, Integer> requests) {
        return executeOnTimeLock(() -> delegate.acquireTimestampLeases(requests));
    }

    @ReviewedRestrictedApiUsage
    @Override
    public Map<TimestampLeaseName, Long> getMinLeasedTimestamps(Set<TimestampLeaseName> timestampNames) {
        return executeOnTimeLock(() -> delegate.getMinLeasedTimestamps(timestampNames));
    }

    private static <T> T executeOnTimeLock(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            if (isAtlasDbDependencyException(e)) {
                throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
            } else {
                throw Throwables.throwUncheckedException(e);
            }
        }
    }

    private static boolean isAtlasDbDependencyException(Exception ex) {
        Throwable cause = ex;
        while (cause != null) {
            if (cause instanceof ConnectException
                    || cause instanceof UnknownHostException
                    || cause instanceof NotCurrentLeaderException) {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

    @Override
    public void close() {
        lockRefresher.close();
        unlocker.close();
        timestampService.close();
    }

    private static LockRefresher<LockToken> createLockRefresher(TimelockService timelockService) {
        LockLeaseRefresher<LockToken> lockTokenRefresher = timelockService::refreshLockLeases;
        return new LockRefresher<>(refreshExecutor, lockTokenRefresher, REFRESH_INTERVAL_MILLIS);
    }

    private static LockRefresher<LockToken> createLockRefresher(
            TimelockService timelockService, long lockRefreshIntervalMillis) {
        LockLeaseRefresher<LockToken> lockTokenRefresher = timelockService::refreshLockLeases;
        return new LockRefresher<>(refreshExecutor, lockTokenRefresher, lockRefreshIntervalMillis);
    }

    private static ScheduledExecutorService createSingleThreadScheduledExecutor(String operation) {
        return PTExecutors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory(TimeLockClient.class.getSimpleName() + "-" + operation, true));
    }

    private static final class TimelockServiceErrorDecorator implements CloseableTimestampService {
        private final TimelockService delegate;

        private TimelockServiceErrorDecorator(TimelockService delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getFreshTimestamp() {
            return executeOnTimeLock(delegate::getFreshTimestamp);
        }

        @Override
        public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
            return executeOnTimeLock(() -> delegate.getFreshTimestamps(numTimestampsRequested));
        }

        @Override
        public void close() {}
    }
}
