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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.nexus.db.sql.AgnosticResultSet;
import com.palantir.nexus.db.sql.SqlConnection;

public final class TableValueStyleCacheImpl implements TableValueStyleCache {
    private static final SafeLogger log = SafeLoggerFactory.get(TableValueStyleCacheImpl.class);

    private final Cache<TableReference, TableValueStyle> valueStyleByTableRef =
            Caffeine.newBuilder().build();

    @Override
    public TableValueStyle getTableType(
            final ConnectionSupplier connectionSupplier, final TableReference tableRef, TableReference metadataTable) {
        try {
            return valueStyleByTableRef.get(tableRef, tableRefKey -> {
                SqlConnection conn = connectionSupplier.get();
                AgnosticResultSet results = conn.selectResultSetUnregisteredQuery(
                        String.format(
                                "SELECT table_size FROM %s WHERE table_name = ?", metadataTable.getQualifiedName()),
                        tableRefKey.getQualifiedName());
                Preconditions.checkArgument(
                        !results.rows().isEmpty(), "table %s not found", tableRefKey.getQualifiedName());

                return TableValueStyle.byId(
                        Iterables.getOnlyElement(results.rows()).getInteger("table_size"));
            });
        } catch (RuntimeException e) {
            log.error(
                    "TableValueStyle for the table {} could not be retrieved.",
                    LoggingArgs.safeInternalTableName(tableRef.getQualifiedName()),
                    e);
            throw e;
        }
    }

    @Override
    public void clearCacheForTable(TableReference tableRef) {
        valueStyleByTableRef.invalidate(tableRef);
    }
}
