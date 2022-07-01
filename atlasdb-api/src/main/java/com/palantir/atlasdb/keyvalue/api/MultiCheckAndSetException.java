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

package com.palantir.atlasdb.keyvalue.api;

import java.util.Map;

public class MultiCheckAndSetException extends RuntimeException {
    private final TableReference tableReference;
    private final byte[] rowName;
    private final Map<Cell, byte[]> expectedValue;
    private final Map<Cell, byte[]> actualValues;

    public MultiCheckAndSetException(
            TableReference tableReference,
            byte[] rowName,
            Map<Cell, byte[]> expectedValue,
            Map<Cell, byte[]> actualValues) {
        super(String.format(
                "Unexpected value observed in table %s while performing multiCheckAndSet."
                        + "If this is happening repeatedly, your program may be out of sync with the database.",
                tableReference));
        this.tableReference = tableReference;
        this.rowName = rowName;
        this.expectedValue = expectedValue;
        this.actualValues = actualValues;
    }

    public TableReference getTableReference() {
        return tableReference;
    }

    public byte[] getRowName() {
        return rowName;
    }

    public Map<Cell, byte[]> getExpectedValue() {
        return expectedValue;
    }

    public Map<Cell, byte[]> getActualValues() {
        return actualValues;
    }
}
