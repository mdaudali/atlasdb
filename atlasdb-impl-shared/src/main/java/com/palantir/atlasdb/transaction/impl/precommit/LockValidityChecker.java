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

package com.palantir.atlasdb.transaction.impl.precommit;

import com.palantir.lock.v2.LockToken;
import java.util.Set;

/**
 * Facade of {@link com.palantir.lock.v2.TimelockService} that only allows for checking that locks are still valid.
 */
public interface LockValidityChecker {
    /**
     * Returns lock tokens from the provided set that were still valid at some point in time during this method call.
     * Note that users must not assume that the locks are still held; validity here implies that the locks were held
     * up to some point in time during this method call.
     */
    Set<LockToken> getStillValidLockTokens(Set<LockToken> tokensToRefresh);
}
