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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.sweep.asts.progress.AbstractDefaultBucketProgressStoreTest;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(DbKvsPostgresExtension.class)
public class DbKvsPostgresDefaultSweepAssignedBucketStoreTest extends AbstractDefaultBucketProgressStoreTest {
    @RegisterExtension
    public static final TestResourceManager TRM = new TestResourceManager(DbKvsPostgresExtension::createKvs);

    public DbKvsPostgresDefaultSweepAssignedBucketStoreTest() {
        super(TRM);
    }
}
