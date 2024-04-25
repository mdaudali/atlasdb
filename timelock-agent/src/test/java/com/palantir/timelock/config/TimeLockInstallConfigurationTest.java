/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.test.utils.SubdirectoryCreator;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import java.io.File;
import java.nio.file.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TimeLockInstallConfigurationTest {
    @TempDir
    public File temporaryFolder;

    private File newPaxosLogDirectory;
    private File newSqliteLogDirectory;
    private File extantPaxosLogDirectory;
    private File extantSqliteLogDirectory;

    @BeforeEach
    public void setUp() {
        newPaxosLogDirectory =
                Paths.get(temporaryFolder.toString(), "part-time-parliament").toFile();
        newSqliteLogDirectory =
                Paths.get(temporaryFolder.toString(), "sqlite-is-cool").toFile();

        extantPaxosLogDirectory = SubdirectoryCreator.createAndGetSubdirectory(temporaryFolder, "lets-do-some-voting");
        extantSqliteLogDirectory = SubdirectoryCreator.createAndGetSubdirectory(temporaryFolder, "whats-a-right-join");
    }

    @Test
    public void testCanReadConfig() {
        File configFile = new File(TimeLockInstallConfiguration.class
                .getResource("/timelock-install.yml")
                .getPath());

        ObjectMapper mapper =
                ObjectMappers.withDefaultModules(YAMLMapper.builder().build());

        assertThatCode(() -> mapper.readValue(configFile, TimeLockInstallConfiguration.class))
                .doesNotThrowAnyException();
    }

    @Test
    public void newServiceIfNewServiceFlagSetToTrue() {
        assertThat(TimeLockInstallConfiguration.builder()
                        .paxos(createPaxosInstall(true, false))
                        .build()
                        .isNewService())
                .isTrue();
    }

    @Test
    public void existingServiceIfNewServiceFlagSetToFalse() {
        assertThat(TimeLockInstallConfiguration.builder()
                        .paxos(createPaxosInstall(false, true))
                        .build()
                        .isNewService())
                .isFalse();
    }

    private PaxosInstallConfiguration createPaxosInstall(boolean isNewService, boolean shouldDirectoriesExist) {
        return createPaxosInstall(isNewService, shouldDirectoriesExist, false);
    }

    private PaxosInstallConfiguration createPaxosInstall(
            boolean isNewService, boolean shouldDirectoriesExist, boolean ignoreCheck) {
        return PaxosInstallConfiguration.builder()
                .dataDirectory(shouldDirectoriesExist ? extantPaxosLogDirectory : newPaxosLogDirectory)
                .sqlitePersistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                        .dataDirectory(shouldDirectoriesExist ? extantSqliteLogDirectory : extantPaxosLogDirectory)
                        .build())
                .isNewService(isNewService)
                .leaderMode(PaxosLeaderMode.SINGLE_LEADER)
                .ignoreNewServiceCheck(ignoreCheck)
                .build();
    }
}
