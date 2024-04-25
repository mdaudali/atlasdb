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

import com.palantir.atlasdb.workload.migration.cql.CassandraKeyspaceReplicationStrategyManager;
import com.palantir.atlasdb.workload.migration.jmx.CassandraStateManager;
import java.util.Set;

public class AlterKeyspaceDatacenters implements MigrationAction {
    private final CassandraKeyspaceReplicationStrategyManager replicationStrategyManager;
    private final CassandraStateManager stateManager;
    private final Set<String> datacentres;
    private final String keyspace;

    public AlterKeyspaceDatacenters(
            CassandraKeyspaceReplicationStrategyManager replicationStrategyManager,
            CassandraStateManager stateManager,
            Set<String> datacenters,
            String keyspace) {
        this.replicationStrategyManager = replicationStrategyManager;
        this.stateManager = stateManager;
        this.keyspace = keyspace;
        this.datacentres = datacenters;
    }

    @Override
    public void runForwardStep() {
        replicationStrategyManager.setReplicationFactorToThreeForDatacenters(datacentres, keyspace);
        stateManager.waitForConsensusSchemaVersionFromNode();
    }

    @Override
    public boolean isApplied() {
        return false;
    }
}
