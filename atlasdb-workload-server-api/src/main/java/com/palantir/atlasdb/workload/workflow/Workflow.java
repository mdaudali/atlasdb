/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.workflow;

import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransaction;
import java.util.List;

public interface Workflow {
    /**
     * Runs desired workflow until completion. Returns a list of {@link WitnessedTransaction}, that corresponds to
     * transactions that were known to be completed.
     * This list is sorted by ascending "timeline timestamp", which is taken to be the commit timestamp in the case
     * of read-write transactions, and the start timestamp in the case of read-only transactions.
     */
    List<WitnessedTransaction> run();
}
