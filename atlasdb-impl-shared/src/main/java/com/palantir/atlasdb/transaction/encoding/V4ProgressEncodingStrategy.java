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

package com.palantir.atlasdb.transaction.encoding;

import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import java.util.Arrays;

public enum V4ProgressEncodingStrategy implements ProgressEncodingStrategy {
    INSTANCE;

    @Override
    public byte[] getInProgressMarker() {
        return TransactionConstants.TTS_IN_PROGRESS_MARKER;
    }

    @Override
    public boolean isInProgress(byte[] value) {
        return Arrays.equals(value, TransactionConstants.TTS_IN_PROGRESS_MARKER);
    }
}