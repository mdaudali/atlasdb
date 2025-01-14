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
package com.palantir.lock.v2;

import com.google.errorprone.annotations.RestrictedApi;
import com.palantir.atlasdb.common.api.annotations.ReviewedRestrictedApiUsage;
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.logsafe.Safe;
import com.palantir.processors.AutoDelegate;
import com.palantir.processors.DoNotDelegate;
import com.palantir.timestamp.TimestampRange;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.ws.rs.QueryParam;

@AutoDelegate
public interface TimelockService {
    /**
     * Used for TimelockServices that can be initialized asynchronously (i.e. those extending
     * {@link com.palantir.async.initializer.AsyncInitializer}); other TimelockServices can keep the default
     * implementation, and return true (they're trivially fully initialized).
     *
     * @return true iff the TimelockService has been fully initialized and is ready to use
     */
    @DoNotDelegate
    default boolean isInitialized() {
        return true;
    }

    long getFreshTimestamp();

    long getCommitTimestamp(long startTs, LockToken commitLocksToken);

    TimestampRange getFreshTimestamps(@Safe @QueryParam("number") int numTimestampsRequested);

    // TODO(gs): Deprecate this once users outside of Atlas transactions have been eliminated
    LockImmutableTimestampResponse lockImmutableTimestamp();

    @DoNotDelegate
    default List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
        // Track these separately in the case that getFreshTimestamp fails but lockImmutableTimestamp succeeds
        List<LockImmutableTimestampResponse> immutableTimestampLocks = new ArrayList<>();
        List<StartIdentifiedAtlasDbTransactionResponse> responses = new ArrayList<>();
        try {
            IntStream.range(0, count).forEach($ -> {
                LockImmutableTimestampResponse immutableTimestamp = lockImmutableTimestamp();
                immutableTimestampLocks.add(immutableTimestamp);
                responses.add(StartIdentifiedAtlasDbTransactionResponse.of(
                        immutableTimestamp, TimestampAndPartition.of(getFreshTimestamp(), 0)));
            });
            return responses;
        } catch (RuntimeException | Error throwable) {
            try {
                unlock(immutableTimestampLocks.stream()
                        .map(LockImmutableTimestampResponse::getLock)
                        .collect(Collectors.toSet()));
            } catch (Throwable unlockThrowable) {
                throwable.addSuppressed(unlockThrowable);
            }
            throw throwable;
        }
    }

    long getImmutableTimestamp();

    LockResponse lock(LockRequest request);

    /**
     * Similar to {@link #lock(LockRequest)}, but will attempt to respect the provided
     * {@link ClientLockingOptions}. Support for these options is not guaranteed in legacy lock configurations.
     */
    LockResponse lock(LockRequest lockRequest, ClientLockingOptions options);

    WaitForLocksResponse waitForLocks(WaitForLocksRequest request);

    Set<LockToken> refreshLockLeases(Set<LockToken> tokens);

    /**
     * Releases locks associated with the set of {@link LockToken}s provided.
     * The set of tokens returned are the tokens for which the associated locks were unlocked in this call.
     * It is possible that a token that was provided is NOT in the returned set (e.g. if it expired).
     * However, in this case it is guaranteed that that token is no longer valid.
     *
     * @param tokens Tokens for which associated locks should be unlocked.
     * @return Tokens for which associated locks were unlocked
     */
    Set<LockToken> unlock(Set<LockToken> tokens);

    /**
     * A version of {@link TimelockService#unlock(Set)} where one does not need to know whether the locks associated
     * with the provided tokens were successfully unlocked or not.
     *
     * In some implementations, this may be more performant than a standard unlock.
     *
     * @param tokens Tokens for which associated locks should be unlocked.
     */
    void tryUnlock(Set<LockToken> tokens);

    long currentTimeMillis();

    /**
     * Acquires a lease on named timestamps. The lease is taken out with a new fresh timestamp.
     * The timestamps supplied are fresh timestamps obtained strictly after the lease is taken out.
     * The supplier returns exactly the number of timestamps requested and throws on any additional
     * interactions.
     */
    @RestrictedApi(
            explanation =
                    "This method is for internal Atlas and internal library use only. Clients MUST NOT use it unless"
                        + " given explicit approval. Mis-use can result in SEVERE DATA CORRUPTION and the API contract"
                        + " is subject to change at any time.",
            allowlistAnnotations = ReviewedRestrictedApiUsage.class)
    TimestampLeaseResults acquireTimestampLeases(Map<TimestampLeaseName, Integer> requests);

    /**
     * Returns the smallest leased timestamp for each of the associated named collections at the time of the call.
     * If there are no active leases, a fresh timestamp is obtained and returned.
     */
    @RestrictedApi(
            explanation =
                    "This method is for internal Atlas and internal library use only. Clients MUST NOT use it unless"
                        + " given explicit approval. Mis-use can result in SEVERE DATA CORRUPTION and the API contract"
                        + " is subject to change at any time.",
            allowlistAnnotations = ReviewedRestrictedApiUsage.class)
    Map<TimestampLeaseName, Long> getMinLeasedTimestamps(Set<TimestampLeaseName> timestampNames);
}
