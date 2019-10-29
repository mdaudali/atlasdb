/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.util;

import static org.assertj.core.api.Assertions.assertThat;

import feign.RetryableException;

public final class ExceptionMatchers {

    private ExceptionMatchers() { }

    public static void isRetryableExceptionWhereLeaderCannotBeFound(Throwable throwable) {
        assertThat(throwable)
                .hasMessageContaining("method invoked on a non-leader");

        // We shade Feign, so we can't rely on our client's RetryableException exactly matching ours.
        assertThat(throwable.getClass().getName())
                .contains("RetryableException");
    }

    public static void shouldRetryOnAnotherNode(Throwable throwable) {
        assertThat(throwable)
                .hasMessageContaining("receiving QosException.RetryOther")
                .isInstanceOf(RetryableException.class);

    }
}
