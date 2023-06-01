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

package com.palantir.atlasdb.workload.transaction;

import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadColumnRangeSelection;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedRowsColumnRangeExhaustionTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.ImmutableWitnessedRowsColumnRangeReadTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowsColumnRangeExhaustionTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedRowsColumnRangeReadTransactionAction;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public interface RowsColumnRangeReadTransactionAction extends TransactionAction {
    /** Table to apply the row range read to. */
    String table();

    /** Rows which should be read. */
    List<Integer> rows();

    /** Column range which should be read. */
    WorkloadColumnRangeSelection batchColumnRangeSelection();

    @Override
    default <T> T accept(TransactionActionVisitor<T> visitor) {
        return visitor.visit(this);
    }

    default WitnessedRowsColumnRangeExhaustionTransactionAction exhaustionWitness(int specificRow) {
        return ImmutableWitnessedRowsColumnRangeExhaustionTransactionAction.builder()
                .originalAction(this)
                .specificRow(specificRow)
                .build();
    }

    default WitnessedRowsColumnRangeReadTransactionAction readWitness(int specificRow, WorkloadCell cell, int value) {
        return ImmutableWitnessedRowsColumnRangeReadTransactionAction.builder()
                .originalAction(this)
                .cell(cell)
                .value(value)
                .specificRow(specificRow)
                .build();
    }
}
