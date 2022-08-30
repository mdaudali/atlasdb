/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.queue;

import java.util.List;
import java.util.Map;
import org.derive4j.Data;

@Data
public interface PartitionedWriteInfo {
    /**
     * If a transaction only writes to non-sweepable tables, we only need the start timestamp. If there are one or more
     * writes to sweepable tables, then we filter out the non-sweepable writes and group the writes.
     */
    interface Cases<R> {
        R nonSweepableTransaction(long startTimestamp);

        R filteredSweepableTransaction(Map<PartitionInfo, List<WriteInfo>> partitionedWrites);
    }

    <R> R match(Cases<R> cases);
}
