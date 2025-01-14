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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Map;
import java.util.Set;

public class GetAsyncExpectationAwareDelegate extends ForwardingExpectationsAwareTransaction {
    private final ExpectationsAwareTransaction delegate;
    private final PathTypeTracker tracker;

    public GetAsyncExpectationAwareDelegate(ExpectationsAwareTransaction transaction) {
        this.delegate = transaction;
        this.tracker = PathTypeTrackers.NO_OP;
    }

    public GetAsyncExpectationAwareDelegate(ExpectationsAwareTransaction transaction, PathTypeTracker tracker) {
        this.delegate = transaction;
        this.tracker = tracker;
    }

    @Override
    public ExpectationsAwareTransaction delegate() {
        return delegate;
    }

    @Override
    public Map<Cell, byte[]> get(TableReference tableRef, Set<Cell> cells) {
        try (PathTypeTracker resource = tracker.enterAsyncPath()) {
            return AtlasFutures.getUnchecked(delegate().getAsync(tableRef, cells));
        }
    }
}
