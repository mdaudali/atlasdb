/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.watch;

import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.common.primitives.Ints;
import com.palantir.lock.LockDescriptor;

/**
 * Note on concurrency:
 *
 * Multiple threads may simultaneously try to update the tracker, potentially with overlapping updates. At the same
 * time, it is allowed to return a slightly stale version on calls to currentState, so we do not want to block on
 * updates. This is implemented as follows.
 *
 * 1. Updates are synchronized, and the values of watches, singleLocks, openLocksEvents, lastKnownVersion, and leaderId
 *    are only changed within the synchronized block. This ensures that two threads are not going to conflict during an
 *    update, or do unnecessary work.
 * 2. Furthermore, changes to the values of watches, singleLocks, openLocksEvents, lastKnownVersion are also guarded by
 *    a write lock. This allows calls to currentState() to return a consistent version of lock watch state even if an
 *    update is in progress, as long as the write lock is not locked
 */
@ThreadSafe
public class LockWatchEventTrackerImpl implements LockWatchEventTracker {
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicReference<RangeSet<LockDescriptor>> watches = new AtomicReference<>(TreeRangeSet.create());
    private final AtomicReference<Map<LockDescriptor, LockWatchInfo>> singleLocks = new AtomicReference<>(
            ImmutableMap.of());
    private final ConcurrentSkipListSet<UUID> openLocksEvents = new ConcurrentSkipListSet<>();
    private final ConcurrentMap<Long, VersionedLockWatchState> startTsToLockWatchState = Maps.newConcurrentMap();
    private volatile OptionalLong lastKnownVersion = OptionalLong.empty();
    private volatile UUID leaderId = UUID.randomUUID();

    @Override
    public VersionedLockWatchState currentState() {
        lock.readLock().lock();
        try {
            return new VersionedLockWatchStateImpl(lastKnownVersion, watches.get(), singleLocks.get(), leaderId);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public synchronized VersionedLockWatchState updateState(LockWatchStateUpdate update) {
        if (!update.success()) {
            resetAllState(update);
            return currentState();
        }

        if (leaderId != update.leaderId()) {
            runUpdate(true, update);
            return currentState();
        }

        if (!lastKnownVersion.isPresent()) {
            resetAllState(update);
            return currentState();
        }

        long currentTrackedVersion = lastKnownVersion.getAsLong();
        if (updateNotPossible(update, currentTrackedVersion)) {
            return currentState();
        }

        runUpdate(false, update);
        return currentState();
    }

    private void resetAllState(LockWatchStateUpdate update) {
        lock.writeLock().lock();
        try {
            openLocksEvents.clear();
            watches.set(TreeRangeSet.create());
            singleLocks.set(ImmutableMap.of());
            lastKnownVersion = update.lastKnownVersion();
            leaderId = update.leaderId();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void runUpdate(boolean resetState, LockWatchStateUpdate update) {
        TreeRangeSet<LockDescriptor> watchesToUpdate;
        Map<LockDescriptor, LockWatchInfo> locksToUpdate;
        int firstUpdateIndex;

        if (resetState) {
            openLocksEvents.clear();
            watchesToUpdate = TreeRangeSet.create();
            locksToUpdate = new HashMap<>();
            firstUpdateIndex = 0;
        } else {
            watchesToUpdate = TreeRangeSet.create(watches.get());
            locksToUpdate = new HashMap<>(singleLocks.get());
            firstUpdateIndex = Ints.saturatedCast(lastKnownVersion.getAsLong() + 1 - update.events().get(0).sequence());
        }

        LockWatchStateUpdater visitor = new LockWatchStateUpdater(watchesToUpdate, locksToUpdate, openLocksEvents);
        update.events().listIterator(firstUpdateIndex).forEachRemaining(event -> event.accept(visitor));
        setAll(watchesToUpdate, locksToUpdate, update);
    }

    private void setAll(TreeRangeSet<LockDescriptor> updatedWatches, Map<LockDescriptor, LockWatchInfo> updatedLocks,
            LockWatchStateUpdate update) {
        lock.writeLock().lock();
        try {
            watches.set(updatedWatches);
            singleLocks.set(updatedLocks);
            lastKnownVersion = update.lastKnownVersion();
            leaderId = update.leaderId();
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean updateNotPossible(LockWatchStateUpdate update, long currentTrackedVersion) {
        return update.events().isEmpty()
                || staleUpdate(update, currentTrackedVersion)
                || versionDiscontinuity(update, currentTrackedVersion);
    }

    private boolean staleUpdate(LockWatchStateUpdate update, long currentTrackedVersion) {
        return update.lastKnownVersion().getAsLong() <= currentTrackedVersion;
    }

    private boolean versionDiscontinuity(LockWatchStateUpdate update, long currentTrackedVersion) {
        return update.events().get(0).sequence() > currentTrackedVersion + 1;
    }

    @Override
    public void setLockWatchStateForStartTimestamp(long startTimestamp, VersionedLockWatchState lockWatchState) {
        startTsToLockWatchState.put(startTimestamp, lockWatchState);
    }

    @Override
    public VersionedLockWatchState getLockWatchStateForStartTimestamp(long startTimestamp) {
        return startTsToLockWatchState.get(startTimestamp);
    }

    @Override
    public VersionedLockWatchState removeLockWatchStateForStartTimestamp(long startTimestamp) {
        return startTsToLockWatchState.remove(startTimestamp);
    }
}
