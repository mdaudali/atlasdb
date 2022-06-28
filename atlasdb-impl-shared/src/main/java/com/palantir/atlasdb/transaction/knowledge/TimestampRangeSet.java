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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableTimestampRangeSet.class)
@JsonDeserialize(as = ImmutableTimestampRangeSet.class)
@SuppressWarnings("UnstableApiUsage") // RangeSet usage
public interface TimestampRangeSet {
    @Value.Parameter
    ImmutableRangeSet<Long> timestampRanges();

    default boolean encloses(Range<Long> timestampRange) {
        return timestampRanges().encloses(timestampRange);
    }

    default TimestampRangeSet copyAndAdd(Range<Long> additionalTimestampRange) {
        ImmutableRangeSet<Long> newTimestampRanges = ImmutableRangeSet.<Long>builder()
                .addAll(timestampRanges())
                .add(additionalTimestampRange)
                .build();
        return ImmutableTimestampRangeSet.builder().timestampRanges(newTimestampRanges).build();
    }

    static TimestampRangeSet singleRange(Range<Long> timestampRange) {
        return ImmutableTimestampRangeSet.builder().timestampRanges(ImmutableRangeSet.of(timestampRange)).build();
    }

    static TimestampRangeSet empty() {
        return ImmutableTimestampRangeSet.builder().timestampRanges(ImmutableRangeSet.of()).build();
    }
}
