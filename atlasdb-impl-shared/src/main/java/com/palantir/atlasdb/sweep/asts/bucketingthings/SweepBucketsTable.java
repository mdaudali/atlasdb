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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import com.palantir.atlasdb.sweep.asts.Bucket;
import com.palantir.atlasdb.sweep.asts.SweepableBucket;
import com.palantir.atlasdb.sweep.asts.SweepableBucket.TimestampRange;
import java.util.Optional;
import java.util.Set;

public interface SweepBucketsTable {
    /**
     * Returns SweepableBuckets for each ShardAndStrategy given in the input set _where_ a sweepable bucket exists.
     * It is not guaranteed that a SweepableBucket exists for each ShardAndStrategy in the input set.
     */
    Set<SweepableBucket> getSweepableBuckets(Set<Bucket> startBuckets);

    void putTimestampRangeForBucket(
            Bucket bucket, Optional<TimestampRange> oldTimestampRange, TimestampRange newTimestampRange);
}