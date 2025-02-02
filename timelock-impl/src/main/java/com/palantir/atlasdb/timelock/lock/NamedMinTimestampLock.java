/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock;

import com.palantir.lock.LockDescriptor;
import java.util.UUID;
import javax.ws.rs.NotSupportedException;

final class NamedMinTimestampLock implements AsyncLock {
    private final NamedMinTimestampTracker tracker;
    private final long timestamp;
    private final LockDescriptor descriptor;

    private NamedMinTimestampLock(NamedMinTimestampTracker tracker, long timestamp, LockDescriptor descriptor) {
        this.tracker = tracker;
        this.timestamp = timestamp;
        this.descriptor = descriptor;
    }

    static NamedMinTimestampLock create(NamedMinTimestampTracker tracker, long timestamp) {
        return new NamedMinTimestampLock(tracker, timestamp, tracker.getDescriptor(timestamp));
    }

    @Override
    public AsyncResult<Void> lock(UUID requestId) {
        tracker.lock(timestamp, requestId);
        return AsyncResult.completedResult();
    }

    @Override
    public AsyncResult<Void> waitUntilAvailable(UUID requestId) {
        throw new NotSupportedException();
    }

    @Override
    public void unlock(UUID requestId) {
        tracker.unlock(timestamp, requestId);
    }

    @Override
    public void timeout(UUID requestId) {
        throw new NotSupportedException();
    }

    @Override
    public LockDescriptor getDescriptor() {
        return descriptor;
    }
}
