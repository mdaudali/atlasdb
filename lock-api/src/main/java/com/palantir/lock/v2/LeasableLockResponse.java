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

package com.palantir.lock.v2;

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonSerialize(as = ImmutableLeasableLockResponse.class)
@JsonDeserialize(as = ImmutableLeasableLockResponse.class)
public abstract class LeasableLockResponse {
    @Value.Parameter
    public abstract Optional<LockToken> getTokenOrEmpty();

    @Value.Parameter
    public abstract Optional<Lease> getLeaseOrEmpty();

    @JsonIgnore
    public boolean wasSuccessful() {
        return getTokenOrEmpty().isPresent();
    }

    @JsonIgnore
    public LockToken getToken() {
        if (!wasSuccessful()) {
            throw new IllegalStateException("This lock response was not successful");
        }
        return getTokenOrEmpty().get();
    }

    @JsonIgnore
    public Lease getLease() {
        if (!wasSuccessful()) {
            throw new IllegalStateException("This lock response was not successful");
        }
        return getLeaseOrEmpty().get();
    }

    public static LeasableLockResponse successful(LockToken token, Lease lease) {
        return ImmutableLeasableLockResponse.of(Optional.of(token), Optional.of(lease));
    }

    public static LeasableLockResponse timedOut() {
        return ImmutableLeasableLockResponse.of(Optional.empty(), Optional.empty());
    }
}
