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

package com.palantir.atlasdb.workload.logging;

// CHECKSTYLE:OFF: BanLoggingImplementations
import ch.qos.logback.classic.AsyncAppender;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.LoggerFactory;

public final class LoggingUtils {
    private LoggingUtils() {}

    public static void setSynchronousLogging() {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger rootLogger = loggerContext.getLogger(Logger.ROOT_LOGGER_NAME);

        List<AsyncAppender> asyncAppenders = getAsyncAppenders(rootLogger);

        asyncAppenders.forEach(asyncAppender -> {
            rootLogger.detachAppender(asyncAppender);

            asyncAppender.iteratorForAppenders().forEachRemaining(wrappedSyncAppender -> {
                rootLogger.addAppender(wrappedSyncAppender);
                wrappedSyncAppender.start();
            });
        });
    }

    private static List<AsyncAppender> getAsyncAppenders(Logger rootLogger) {
        List<AsyncAppender> asyncAppenders = new ArrayList<>();
        for (Iterator<Appender<ILoggingEvent>> iterator = rootLogger.iteratorForAppenders(); iterator.hasNext(); ) {
            Appender<ILoggingEvent> appender = iterator.next();
            if (appender instanceof AsyncAppender) {
                asyncAppenders.add((AsyncAppender) appender);
            }
        }
        return asyncAppenders;
    }
}
