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

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public final class CqlUtilitiesTest {

    @Test
    public void encodedBytesArePrefixedCorrectlyAndHaveCorrectHexValues() {
        assertThat(CqlUtilities.encodeCassandraHexBytes(new byte[] {0, 1, 2})).isEqualTo("0x000102");
        assertThat(CqlUtilities.encodeCassandraHexBytes(new byte[] {10, 12, 15}))
                .isEqualTo("0x0A0C0F");
    }

    @Test
    public void encodedStringsArePrefixedCorrectlyAndHaveCorrectHexValues() {
        assertThat(CqlUtilities.encodeCassandraHexString("cas")).isEqualTo("0x636173");
        assertThat(CqlUtilities.encodeCassandraHexString("jklmnop")).isEqualTo("0x6A6B6C6D6E6F70");
    }
}
