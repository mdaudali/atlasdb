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

package com.palantir.atlasdb.timelock.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.conjure.java.serialization.ObjectMappers;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public class TimestampLeaseRequestsTest {
    private static final ObjectMapper SERIALIZATION_MAPPER =
            ObjectMappers.newClientObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final ObjectMapper DESERIALIZATION_MAPPER =
            ObjectMappers.newServerObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    @Test
    void canSerializeAndDeserialize() throws IOException {
        Namespace namespace1 = Namespace.of("timelock-client-namespace");
        TimestampLeaseRequests leaseRequests1 = TimestampLeaseRequests.of(
                RequestId.of(new UUID(0, 0)),
                Map.of(
                        TimestampLeaseName.of("timestamp-lease-name-1"), 10,
                        TimestampLeaseName.of("timestamp-lease-name-2"), 20));

        Namespace namespace2 = Namespace.of("another-timelock-client-namespace");
        TimestampLeaseRequests leaseRequests2 = TimestampLeaseRequests.of(
                RequestId.of(new UUID(0, 1)), Map.of(TimestampLeaseName.of("another-timestamp-lease-name"), 30));

        MultiClientTimestampLeaseRequest request = MultiClientTimestampLeaseRequest.of(Map.of(
                namespace1,
                NamespaceTimestampLeaseRequest.of(List.of(leaseRequests1)),
                namespace2,
                NamespaceTimestampLeaseRequest.of(List.of(leaseRequests2))));
        byte[] asBytes = SERIALIZATION_MAPPER.writeValueAsBytes(request);
        MultiClientTimestampLeaseRequest deserialized =
                DESERIALIZATION_MAPPER.readValue(asBytes, MultiClientTimestampLeaseRequest.class);
        assertThat(deserialized).isEqualTo(request);
    }
}
