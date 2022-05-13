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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.CqlQuerySpec;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class ThrowingCqlClient implements CqlClient {

    @Override
    public <V> ListenableFuture<V> executeQuery(CqlQuerySpec<V> querySpec) {
        throw new SafeIllegalStateException("CQL config invalid or not provided.");
    }

    public static ThrowingCqlClient of() {
        return new ThrowingCqlClient();
    }

    @Override
    public void close() {
        // no-op, nothing to close
    }

    @Override
    public boolean isValid() {
        return false;
    }
}
