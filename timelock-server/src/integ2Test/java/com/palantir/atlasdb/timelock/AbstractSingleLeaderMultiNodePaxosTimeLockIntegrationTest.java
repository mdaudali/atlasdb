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

package com.palantir.atlasdb.timelock;

import static com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE;
import static com.palantir.atlasdb.timelock.paxos.PaxosUseCase.LEADER_FOR_ALL_CLIENTS;
import static com.palantir.atlasdb.timelock.paxos.PaxosUseCase.PSEUDO_LEADERSHIP_CLIENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.paxos.BatchPaxosAcceptorRpcClient;
import com.palantir.atlasdb.timelock.paxos.PaxosRemoteClients;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import com.palantir.atlasdb.timelock.util.TestProxies;
import com.palantir.paxos.Client;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class AbstractSingleLeaderMultiNodePaxosTimeLockIntegrationTest {
    private static final ImmutableSet<Client> CLIENT_SET = ImmutableSet.of(PSEUDO_LEADERSHIP_CLIENT);

    private final TestableTimelockCluster cluster;

    private NamespacedClients namespace;

    public AbstractSingleLeaderMultiNodePaxosTimeLockIntegrationTest(TestableTimelockCluster cluster) {
        this.cluster = cluster;
    }

    @BeforeEach
    public void setUp() {
        namespace = cluster.clientForRandomNamespace().throughWireMockProxy();
    }

    @Test
    public void clientsCreatedDynamicallyOnNonLeadersAreFunctionalAfterFailover() {
        cluster.nonLeaders(namespace.namespace()).forEach((clientName, server) -> assertThatThrownBy(
                        () -> server.client(clientName).getFreshTimestamp())
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound));

        cluster.failoverToNewLeader(namespace.namespace());

        namespace.getFreshTimestamp();
    }

    @Test
    public void clientsCreatedDynamicallyOnLeaderAreFunctionalImmediately() {
        assertThatCode(() -> cluster.currentLeaderFor(namespace.namespace())
                        .client(namespace.namespace())
                        .getFreshTimestamp())
                .doesNotThrowAnyException();
    }

    @Test
    public void noConflictIfLeaderAndNonLeadersSeparatelyInitializeClient() {
        cluster.nonLeaders(namespace.namespace()).forEach((clientName, server) -> assertThatThrownBy(
                        () -> server.client(clientName).getFreshTimestamp())
                .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound));

        long ts1 = namespace.getFreshTimestamp();

        cluster.failoverToNewLeader(namespace.namespace());

        long ts2 = namespace.getFreshTimestamp();
        assertThat(ts1).isLessThan(ts2);
    }

    @Test
    public void migrationToBatchedSingleLeaderHasConsistentSequenceNumbers() {
        NamespacedClients client = cluster.clientForRandomNamespace().throughWireMockProxy();
        cluster.waitUntilAllServersOnlineAndReadyToServeNamespaces(ImmutableList.of(client.namespace()));

        List<Long> sequenceNumbers = new ArrayList<>();

        for (TestableTimelockServer server : cluster.servers()) {
            server.startUsingBatchedSingleLeader();
            if (cluster.currentLeaderFor(client.namespace()) == server) {
                // if we are the leader failover twice to ensure we see the new sequence
                cluster.failoverToNewLeader(client.namespace());
            }
            cluster.failoverToNewLeader(client.namespace());
            long sequenceForBatchedEndpoint = getSequenceForServerUsingBatchedEndpoint(server);
            long sequenceForOldEndpoint = getSequenceForServerUsingOldEndpoint(server);
            assertThat(sequenceForBatchedEndpoint).isLessThanOrEqualTo(sequenceForOldEndpoint);
            sequenceNumbers.add(sequenceForBatchedEndpoint);
        }

        assertThat(sequenceNumbers).isSorted();
        assertThat(ImmutableSet.copyOf(sequenceNumbers)).hasSameSizeAs(sequenceNumbers);
    }

    @Test
    public void reverseMigrationFromBatchedSingleLeaderHasConsistentSequenceNumbers() {
        NamespacedClients client = cluster.clientForRandomNamespace().throughWireMockProxy();
        cluster.waitUntilAllServersOnlineAndReadyToServeNamespaces(ImmutableList.of(client.namespace()));
        cluster.servers().forEach(TestableTimelockServer::startUsingBatchedSingleLeader);
        List<Long> sequenceNumbers = new ArrayList<>();

        for (TestableTimelockServer server : cluster.servers()) {
            server.stopUsingBatchedSingleLeader();
            if (cluster.currentLeaderFor(client.namespace()) == server) {
                // if we are the leader failover twice to ensure we see the new sequence
                cluster.failoverToNewLeader(client.namespace());
            }
            cluster.failoverToNewLeader(client.namespace());
            long sequenceForBatchedEndpoint = getSequenceForServerUsingBatchedEndpoint(server);
            long sequenceForOldEndpoint = getSequenceForServerUsingOldEndpoint(server);
            assertThat(sequenceForBatchedEndpoint).isLessThanOrEqualTo(sequenceForOldEndpoint);
            sequenceNumbers.add(sequenceForBatchedEndpoint);
        }

        assertThat(sequenceNumbers).isSorted();
        assertThat(ImmutableSet.copyOf(sequenceNumbers)).hasSameSizeAs(sequenceNumbers);
    }

    private static long getSequenceForServerUsingBatchedEndpoint(TestableTimelockServer server) {
        BatchPaxosAcceptorRpcClient acceptor = server.client(LEADER_PAXOS_NAMESPACE)
                .proxyFactory()
                .createProxy(BatchPaxosAcceptorRpcClient.class, TestProxies.ProxyMode.DIRECT);
        return acceptor.latestSequencesPreparedOrAccepted(LEADER_FOR_ALL_CLIENTS, null, CLIENT_SET)
                .updates()
                .get(PSEUDO_LEADERSHIP_CLIENT);
    }

    private static long getSequenceForServerUsingOldEndpoint(TestableTimelockServer server) {
        PaxosRemoteClients.TimelockSingleLeaderPaxosAcceptorRpcClient acceptor = server.client(LEADER_PAXOS_NAMESPACE)
                .proxyFactory()
                .createProxy(
                        PaxosRemoteClients.TimelockSingleLeaderPaxosAcceptorRpcClient.class,
                        TestProxies.ProxyMode.DIRECT);
        return acceptor.getLatestSequencePreparedOrAccepted();
    }
}
