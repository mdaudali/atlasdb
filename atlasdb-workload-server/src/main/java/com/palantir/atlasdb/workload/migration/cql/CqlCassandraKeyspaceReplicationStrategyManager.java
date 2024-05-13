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
package com.palantir.atlasdb.workload.migration.cql;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.cassandra.manager.core.cql.ImmutableKeyspaceQuery;
import com.palantir.cassandra.manager.core.cql.KeyspaceQuery;
import com.palantir.cassandra.manager.core.cql.KeyspaceQueryMethod;
import com.palantir.cassandra.manager.core.cql.ReplicationOptions;
import com.palantir.cassandra.manager.core.cql.SchemaMutationResult;
import com.palantir.cassandra.manager.objects.SafeKeyspace;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import one.util.streamex.StreamEx;

public class CqlCassandraKeyspaceReplicationStrategyManager implements CassandraKeyspaceReplicationStrategyManager {
    private static final SafeLogger log = SafeLoggerFactory.get(CqlCassandraKeyspaceReplicationStrategyManager.class);
    private static final String TOPOLOGY_STRATEGY_KEY = "class";
    private static final String NETWORK_TOPOLOGY_STRATEGY = "NetworkTopologyStrategy";
    public static final Set<String> SYSTEM_KEYSPACES =
            ImmutableSet.of("system", "system_auth", "system_distributed", "system_traces");
    public static final ImmutableSet<String> KEYSPACE_IGNORE_LIST = Sets.union(
                    SYSTEM_KEYSPACES, ImmutableSet.of("__simple_rf_test_keyspace__"))
            .immutableCopy();

    public static final Integer RF = 3;

    private final Supplier<Session> sessionProvider;

    public CqlCassandraKeyspaceReplicationStrategyManager(Supplier<Session> sessionProvider) {
        this.sessionProvider = sessionProvider;
    }

    @Override
    public SchemaMutationResult setReplicationFactorToThreeForDatacenters(Set<String> datacenters, String keyspace) {
        getNonSystemKeyspaces();
        Map<String, String> datacenterReplicationFactor = StreamEx.of(datacenters)
                .mapToEntry(_datacenter -> RF.toString())
                .append(TOPOLOGY_STRATEGY_KEY, NETWORK_TOPOLOGY_STRATEGY)
                .toMap();
        KeyspaceQuery query = ImmutableKeyspaceQuery.builder()
                .keyspace(SafeKeyspace.of(keyspace))
                .durableWrites(true)
                .replication(ReplicationOptions.of(datacenterReplicationFactor))
                .method(KeyspaceQueryMethod.ALTER)
                .build();
        return runWithCqlSession(query::applyTo);
    }

    @Override
    public boolean isReplicationFactorSetToThreeForDatacentersForKeyspace(
            Set<String> datacenters, KeyspaceMetadata keyspace) {
        ReplicationOptions replicationOptions = ReplicationOptions.of(keyspace.getReplication());
        return replicationOptions
                .networkTopologyStrategyOption()
                .map(settings -> StreamEx.of(datacenters)
                        .allMatch(datacenter -> settings.containsKey(datacenter)
                                && RF.equals(settings.get(datacenter))
                                && settings.size() == datacenters.size()))
                .orElse(false);
    }

    @Override
    public Set<KeyspaceMetadata> getNonSystemKeyspaces() {
        List<KeyspaceMetadata> ks =
                runWithCqlSession(session -> session.getCluster().getMetadata().getKeyspaces());
        log.info(
                "All keyspaces {}",
                SafeArg.of("results", ks.stream().map(KeyspaceMetadata::getName).collect(Collectors.toList())));
        return StreamEx.of(ks)
                .remove(keyspaceMetadata -> KEYSPACE_IGNORE_LIST.contains(keyspaceMetadata.getName()))
                .toImmutableSet();
    }

    private <T> T runWithCqlSession(Function<Session, T> sessionConsumer) {
        try (Session session = sessionProvider.get()) {
            return sessionConsumer.apply(session);
        }
    }
}