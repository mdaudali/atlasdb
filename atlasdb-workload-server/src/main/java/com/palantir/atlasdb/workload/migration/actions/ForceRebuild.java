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

package com.palantir.atlasdb.workload.migration.actions;

import com.datastax.driver.core.KeyspaceMetadata;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.workload.migration.cql.CassandraKeyspaceReplicationStrategyManager;
import com.palantir.atlasdb.workload.migration.jmx.CassandraStateManager;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class ForceRebuild implements MigrationAction {
    private final CassandraStateManager dc2StateManager;
    private final CassandraKeyspaceReplicationStrategyManager replicationStrategyManager;
    private final String sourceDatacenter;
    private final Consumer<String> markRebuildAsStarted;

    public ForceRebuild(
            CassandraStateManager dc2StateManager,
            CassandraKeyspaceReplicationStrategyManager replicationStrategyManager,
            Consumer<String> markRebuildAsStarted,
            String sourceDatacenter) {
        this.dc2StateManager = dc2StateManager;
        this.replicationStrategyManager = replicationStrategyManager;
        this.sourceDatacenter = sourceDatacenter;
        this.markRebuildAsStarted = markRebuildAsStarted;
    }

    @Override
    public void runForwardStep() {
        dc2StateManager.forceRebuild(sourceDatacenter, getKeyspaceNames(), markRebuildAsStarted);
    }

    @Override
    public boolean isApplied() {
        // TODO: Skipped other checks
        Set<String> nonSystemKeyspaces = getKeyspaceNames();
        return Sets.difference(nonSystemKeyspaces, dc2StateManager.getRebuiltKeyspaces(sourceDatacenter))
                .isEmpty();
    }

    private Set<String> getKeyspaceNames() {
        return replicationStrategyManager.getNonSystemKeyspaces().stream()
                .map(KeyspaceMetadata::getName)
                .collect(Collectors.toSet());
    }
}
