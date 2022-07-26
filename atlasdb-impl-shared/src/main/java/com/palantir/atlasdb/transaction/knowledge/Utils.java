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

package com.palantir.atlasdb.transaction.knowledge;

import com.palantir.atlasdb.AtlasDbConstants;

public final class Utils {
    private Utils() {}

    public static long getMaxTsInCurrentBucket(long bucket) {
        return getMinTsInBucket(bucket + 1) - 1;
    }

    public static long getBucket(long startTimestamp) {
        return startTimestamp / AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE;
    }

    public static long getMinTsInBucket(long bucket) {
        return bucket * AtlasDbConstants.ABORTED_TIMESTAMPS_BUCKET_SIZE;
    }
}
