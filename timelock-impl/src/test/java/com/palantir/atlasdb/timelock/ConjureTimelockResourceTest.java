/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureLockTokenV2;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureTimestampRange;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.GetMinLeasedNamedTimestampRequests;
import com.palantir.atlasdb.timelock.api.GetMinLeasedNamedTimestampResponses;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseRequests;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseResponse;
import com.palantir.atlasdb.timelock.api.NamedMinTimestampLeaseResponses;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.TimestampName;
import com.palantir.atlasdb.util.TimelockTestUtils;
import com.palantir.common.time.NanoTime;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.conjure.java.undertow.lib.RequestContext;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.impl.TooManyRequestsException;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.timestamp.TimestampRange;
import com.palantir.tokens.auth.AuthHeader;
import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class ConjureTimelockResourceTest {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer test");
    private static final int REMOTE_PORT = 4321;
    private static final URL LOCAL = TimelockTestUtils.url("https://localhost:1234");
    private static final URL REMOTE = TimelockTestUtils.url("https://localhost:" + REMOTE_PORT);
    private static final RedirectRetryTargeter TARGETER =
            RedirectRetryTargeter.create(LOCAL, ImmutableList.of(LOCAL, REMOTE));

    private static final String NAMESPACE = "test";
    private static final RequestContext REQUEST_CONTEXT = null;

    @Mock
    private AsyncTimelockService timelockService;

    @Mock
    private LeaderTime leaderTime;

    private ConjureTimelockResource resource;
    private ConjureTimelockService service;

    @BeforeEach
    public void before() {
        resource = new ConjureTimelockResource(TARGETER, (_namespace, _userAgent) -> timelockService);
        service = ConjureTimelockResource.jersey(TARGETER, (_namespace, _userAgent) -> timelockService);
    }

    @Test
    public void canAcquireNamedMinTimestampLease() {
        NamedMinTimestampLeaseRequest request1 = createNamedMinTimestampLeaseRequest();
        NamedMinTimestampLeaseResponse response1 = createNamedMinTimestampLeaseResponse(request1);
        stubAcquireNamedMinTimestampLeaseInResource(timelockService, request1, response1);

        NamedMinTimestampLeaseRequest request2 = createNamedMinTimestampLeaseRequest();
        NamedMinTimestampLeaseResponse response2 = createNamedMinTimestampLeaseResponse(request2);
        stubAcquireNamedMinTimestampLeaseInResource(timelockService, request2, response2);

        NamedMinTimestampLeaseRequests requests = NamedMinTimestampLeaseRequests.of(List.of(request1, request2));
        NamedMinTimestampLeaseResponses responses = NamedMinTimestampLeaseResponses.of(List.of(response1, response2));
        assertThat(Futures.getUnchecked(resource.acquireNamedMinTimestampLeases(
                        AUTH_HEADER, Namespace.of(NAMESPACE), requests, REQUEST_CONTEXT)))
                .isEqualTo(responses);
    }

    @Test
    public void canGetNamedMinTimestamp() {
        long timestamp1 = 1L;
        TimestampName timestampName1 = TimestampName.of("t1");
        stubGetMinLeasedNamedTimestampInResource(timelockService, timestampName1, timestamp1);

        long timestamp2 = 2L;
        TimestampName timestampName2 = TimestampName.of("t2");
        stubGetMinLeasedNamedTimestampInResource(timelockService, timestampName2, timestamp2);

        GetMinLeasedNamedTimestampRequests requests =
                GetMinLeasedNamedTimestampRequests.of(Set.of(timestampName1, timestampName2));
        GetMinLeasedNamedTimestampResponses responses =
                GetMinLeasedNamedTimestampResponses.of(Map.of(timestampName1, timestamp1, timestampName2, timestamp2));
        assertThat(Futures.getUnchecked(resource.getMinLeasedNamedTimestamps(
                        AUTH_HEADER, Namespace.of(NAMESPACE), requests, REQUEST_CONTEXT)))
                .isEqualTo(responses);
    }

    @Test
    public void canGetLeaderTime() {
        when(timelockService.leaderTime()).thenReturn(Futures.immediateFuture(leaderTime));
        assertThat(Futures.getUnchecked(resource.leaderTime(AUTH_HEADER, NAMESPACE, REQUEST_CONTEXT)))
                .isEqualTo(leaderTime);
    }

    @Test
    public void canGetTimestampsUsingSingularAndBatchedMethods() {
        TimestampRange firstRange = TimestampRange.createInclusiveRange(1L, 2L);
        TimestampRange secondRange = TimestampRange.createInclusiveRange(3L, 4L);
        TimestampRange thirdRange = TimestampRange.createInclusiveRange(5L, 5L);

        when(timelockService.getFreshTimestampsAsync(anyInt()))
                .thenReturn(Futures.immediateFuture(firstRange))
                .thenReturn(Futures.immediateFuture(secondRange))
                .thenReturn(Futures.immediateFuture(thirdRange));

        assertThat(Futures.getUnchecked(resource.getFreshTimestamps(
                        AUTH_HEADER, NAMESPACE, ConjureGetFreshTimestampsRequest.of(2), REQUEST_CONTEXT)))
                .satisfies(response -> {
                    assertThat(response.getInclusiveLower()).isEqualTo(firstRange.getLowerBound());
                    assertThat(response.getInclusiveUpper()).isEqualTo(firstRange.getUpperBound());
                });
        assertThat(Futures.getUnchecked(resource.getFreshTimestampsV2(
                                AUTH_HEADER, NAMESPACE, ConjureGetFreshTimestampsRequestV2.of(2), REQUEST_CONTEXT))
                        .get())
                .satisfies(range -> {
                    assertThat(range.getStart()).isEqualTo(secondRange.getLowerBound());
                    assertThat(range.getCount()).isEqualTo(secondRange.size());
                });
        assertThat(Futures.getUnchecked(resource.getFreshTimestamp(AUTH_HEADER, NAMESPACE, REQUEST_CONTEXT))
                        .get())
                .isEqualTo(thirdRange.getLowerBound());
    }

    @Test
    public void canUnlockUsingV1AndV2Endpoints() {
        UUID tokenOne = UUID.randomUUID();
        UUID tokenTwo = UUID.randomUUID();
        UUID tokenThree = UUID.randomUUID();

        Set<ConjureLockToken> requestOne = ImmutableSet.of(ConjureLockToken.of(tokenOne));

        Set<LockToken> setOne = ImmutableSet.of(LockToken.of(tokenTwo));
        Set<LockToken> setTwo = ImmutableSet.of(LockToken.of(tokenThree));

        when(timelockService.unlock(any()))
                .thenReturn(Futures.immediateFuture(setOne))
                .thenReturn(Futures.immediateFuture(setTwo));

        ConjureUnlockResponse unlockResponse = Futures.getUnchecked(
                resource.unlock(AUTH_HEADER, NAMESPACE, ConjureUnlockRequest.of(requestOne), REQUEST_CONTEXT));
        assertThat(unlockResponse.getTokens()).containsExactly(ConjureLockToken.of(tokenTwo));
        verify(timelockService).unlock(eq(ImmutableSet.of(LockToken.of(tokenOne))));

        ConjureUnlockResponseV2 secondResponse = Futures.getUnchecked(resource.unlockV2(
                AUTH_HEADER,
                NAMESPACE,
                ConjureUnlockRequestV2.of(ImmutableSet.of(ConjureLockTokenV2.of(tokenThree))),
                REQUEST_CONTEXT));

        assertThat(secondResponse.get()).containsExactly(ConjureLockTokenV2.of(tokenThree));
        verify(timelockService).unlock(eq(ImmutableSet.of(LockToken.of(tokenThree))));
    }

    @Test
    public void canRefreshUsingV1AndV2Endpoints() {
        UUID tokenOne = UUID.randomUUID();
        UUID tokenTwo = UUID.randomUUID();
        UUID tokenThree = UUID.randomUUID();

        Set<ConjureLockToken> requestOne = ImmutableSet.of(ConjureLockToken.of(tokenOne));

        Set<LockToken> setOne = ImmutableSet.of(LockToken.of(tokenTwo));
        Set<LockToken> setTwo = ImmutableSet.of(LockToken.of(tokenThree));

        Lease leaseOne =
                Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(1234L)), Duration.ofDays(2000));
        Lease leaseTwo =
                Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(2345L)), Duration.ofDays(3333));

        when(timelockService.refreshLockLeases(any()))
                .thenReturn(Futures.immediateFuture(RefreshLockResponseV2.of(setOne, leaseOne)))
                .thenReturn(Futures.immediateFuture(RefreshLockResponseV2.of(setTwo, leaseTwo)));

        ConjureRefreshLocksResponse refreshResponse = Futures.getUnchecked(resource.refreshLocks(
                AUTH_HEADER, NAMESPACE, ConjureRefreshLocksRequest.of(requestOne), REQUEST_CONTEXT));
        assertThat(refreshResponse.getRefreshedTokens()).containsExactly(ConjureLockToken.of(tokenTwo));
        assertThat(refreshResponse.getLease()).isEqualTo(leaseOne);
        verify(timelockService).refreshLockLeases(eq(ImmutableSet.of(LockToken.of(tokenOne))));

        ConjureRefreshLocksResponseV2 secondResponse = Futures.getUnchecked(resource.refreshLocksV2(
                AUTH_HEADER,
                NAMESPACE,
                ConjureRefreshLocksRequestV2.of(ImmutableSet.of(ConjureLockTokenV2.of(tokenThree))),
                REQUEST_CONTEXT));

        assertThat(secondResponse.getRefreshedTokens()).containsExactly(ConjureLockTokenV2.of(tokenThree));
        assertThat(secondResponse.getLease()).isEqualTo(leaseTwo);
        verify(timelockService).refreshLockLeases(eq(ImmutableSet.of(LockToken.of(tokenThree))));
    }

    @Test
    public void jerseyPropagatesExceptions() {
        when(resource.leaderTime(AUTH_HEADER, NAMESPACE, REQUEST_CONTEXT)).thenThrow(new BlockingTimeoutException(""));
        assertQosExceptionThrownBy(
                Futures.submitAsync(
                        () -> Futures.immediateFuture(service.leaderTime(AUTH_HEADER, NAMESPACE)),
                        MoreExecutors.directExecutor()),
                new AssertVisitor() {
                    @Override
                    public Void visit(QosException.Throttle exception) {
                        assertThat(exception.getRetryAfter()).contains(Duration.ZERO);
                        return null;
                    }
                });
    }

    @Test
    public void handlesBlockingTimeout() {
        when(resource.leaderTime(AUTH_HEADER, NAMESPACE, REQUEST_CONTEXT)).thenThrow(new BlockingTimeoutException(""));
        assertQosExceptionThrownBy(resource.leaderTime(AUTH_HEADER, NAMESPACE, REQUEST_CONTEXT), new AssertVisitor() {
            @Override
            public Void visit(QosException.Throttle exception) {
                assertThat(exception.getRetryAfter()).contains(Duration.ZERO);
                return null;
            }
        });
    }

    @Test
    public void handlesTooManyRequestsException() {
        when(resource.leaderTime(AUTH_HEADER, NAMESPACE, REQUEST_CONTEXT)).thenThrow(new TooManyRequestsException(""));
        assertQosExceptionThrownBy(resource.leaderTime(AUTH_HEADER, NAMESPACE, REQUEST_CONTEXT), new AssertVisitor() {
            @Override
            public Void visit(QosException.Throttle exception) {
                assertThat(exception.getRetryAfter()).isEmpty();
                return null;
            }
        });
    }

    @Test
    public void handlesNotCurrentLeader() {
        when(resource.leaderTime(AUTH_HEADER, NAMESPACE, REQUEST_CONTEXT))
                .thenThrow(new NotCurrentLeaderException("", HostAndPort.fromParts("localhost", REMOTE_PORT)));
        assertQosExceptionThrownBy(resource.leaderTime(AUTH_HEADER, NAMESPACE, REQUEST_CONTEXT), new AssertVisitor() {
            @Override
            public Void visit(QosException.RetryOther exception) {
                assertThat(exception.getRedirectTo()).isEqualTo(REMOTE);
                return null;
            }
        });
    }

    private static void assertQosExceptionThrownBy(ListenableFuture<?> future, AssertVisitor visitor) {
        try {
            Futures.getDone(future);
            fail("Future was expected to error");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertThat(cause).isInstanceOf(QosException.class);
            ((QosException) cause).accept(visitor);
        }
    }

    private static void stubAcquireNamedMinTimestampLeaseInResource(
            AsyncTimelockService service,
            NamedMinTimestampLeaseRequest request,
            NamedMinTimestampLeaseResponse response) {
        when(service.acquireNamedMinTimestampLease(
                        request.getTimestampName(), request.getRequestId(), request.getNumFreshTimestamps()))
                .thenReturn(Futures.immediateFuture(response));
    }

    private static void stubGetMinLeasedNamedTimestampInResource(
            AsyncTimelockService service, TimestampName timestampName, long timestamp) {
        when(service.getMinLeasedNamedTimestamp(timestampName)).thenReturn(Futures.immediateFuture(timestamp));
    }

    private static NamedMinTimestampLeaseRequest createNamedMinTimestampLeaseRequest() {
        return NamedMinTimestampLeaseRequest.builder()
                .timestampName(TimestampName.of(RandomStringUtils.random(32)))
                .requestId(UUID.randomUUID())
                .numFreshTimestamps(createRandomPositiveInteger())
                .build();
    }

    private static NamedMinTimestampLeaseResponse createNamedMinTimestampLeaseResponse(
            NamedMinTimestampLeaseRequest request) {
        return NamedMinTimestampLeaseResponse.builder()
                .minLeased(createRandomPositiveInteger())
                .lock(LockToken.of(request.getRequestId()))
                .lease(Lease.of(
                        LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(1234L)), Duration.ofDays(2000)))
                .freshTimestamps(ConjureTimestampRange.of(createRandomPositiveInteger(), createRandomPositiveInteger()))
                .build();
    }

    private static int createRandomPositiveInteger() {
        return ThreadLocalRandom.current().nextInt(0, 2500);
    }

    private abstract static class AssertVisitor implements QosException.Visitor<Void> {

        @Override
        public Void visit(QosException.Throttle exception) {
            fail("Did not expect throttle");
            return null;
        }

        @Override
        public Void visit(QosException.RetryOther exception) {
            fail("Did not expect retry other");
            return null;
        }

        @Override
        public Void visit(QosException.Unavailable exception) {
            fail("Did not expect unavailable");
            return null;
        }
    }
}
