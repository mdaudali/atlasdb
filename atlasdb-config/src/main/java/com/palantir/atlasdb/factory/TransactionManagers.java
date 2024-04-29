/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.factory;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.async.initializer.AsyncInitializer;
import com.palantir.async.initializer.Callback;
import com.palantir.async.initializer.LambdaCallback;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.cache.DefaultTimestampCache;
import com.palantir.atlasdb.cache.TimestampCache;
import com.palantir.atlasdb.cell.api.TransactionKeyValueServiceManager;
import com.palantir.atlasdb.cleaner.CleanupFollower;
import com.palantir.atlasdb.cleaner.DefaultCleanerBuilder;
import com.palantir.atlasdb.cleaner.Follower;
import com.palantir.atlasdb.cleaner.GlobalClock;
import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cleaner.api.Cleaner;
import com.palantir.atlasdb.compact.BackgroundCompactor;
import com.palantir.atlasdb.compact.CompactorConfig;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.config.AuxiliaryRemotingParameters;
import com.palantir.atlasdb.config.ImmutableAtlasDbConfig;
import com.palantir.atlasdb.config.ServerListConfig;
import com.palantir.atlasdb.config.TimeLockClientConfig;
import com.palantir.atlasdb.config.TimeLockRequestBatcherProviders;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.debug.ConflictTracer;
import com.palantir.atlasdb.debug.LockDiagnosticComponents;
import com.palantir.atlasdb.factory.startup.ConsistencyCheckRunner;
import com.palantir.atlasdb.factory.timestamp.FreshTimestampSupplierAdapter;
import com.palantir.atlasdb.http.AtlasDbRemotingConstants;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TransactionSchemaInstaller;
import com.palantir.atlasdb.internalschema.TransactionSchemaManager;
import com.palantir.atlasdb.internalschema.metrics.MetadataCoordinationServiceMetrics;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.DelegatingTransactionKeyValueServiceManager;
import com.palantir.atlasdb.keyvalue.impl.ProfilingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.TracingKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.ValidatingQueryRewritingKeyValueService;
import com.palantir.atlasdb.logging.KvsProfilingLogger;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.schema.TargetedSweepSchema;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;
import com.palantir.atlasdb.spi.DerivedSnapshotConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceManager;
import com.palantir.atlasdb.spi.SharedResourcesConfig;
import com.palantir.atlasdb.spi.TransactionKeyValueServiceConfig;
import com.palantir.atlasdb.spi.TransactionKeyValueServiceManagerFactory;
import com.palantir.atlasdb.spi.TransactionKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.sweep.queue.MultiTableSweepQueueWriter;
import com.palantir.atlasdb.sweep.queue.TargetedSweeper;
import com.palantir.atlasdb.sweep.queue.clear.SafeTableClearerKeyValueService;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepRuntimeConfig;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.adjudicate.feedback.TimeLockClientFeedbackService;
import com.palantir.atlasdb.transaction.ImmutableTransactionConfig;
import com.palantir.atlasdb.transaction.TransactionConfig;
import com.palantir.atlasdb.transaction.api.AtlasDbConstraintCheckingMode;
import com.palantir.atlasdb.transaction.api.LockWatchingCache;
import com.palantir.atlasdb.transaction.api.NoOpLockWatchingCache;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManager;
import com.palantir.atlasdb.transaction.impl.ConflictDetectionManagers;
import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManager;
import com.palantir.atlasdb.transaction.impl.SweepStrategyManagers;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.impl.consistency.ImmutableTimestampCorroborationConsistencyCheck;
import com.palantir.atlasdb.transaction.impl.metrics.DefaultMetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.impl.metrics.MetricsFilterEvaluationContext;
import com.palantir.atlasdb.transaction.knowledge.AbandonedTimestampStoreImpl;
import com.palantir.atlasdb.transaction.knowledge.TransactionKnowledgeComponents;
import com.palantir.atlasdb.transaction.knowledge.coordinated.CoordinationAwareKnownAbandonedTransactionsStore;
import com.palantir.atlasdb.transaction.service.TransactionService;
import com.palantir.atlasdb.transaction.service.TransactionServices;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.common.annotation.Output;
import com.palantir.common.annotations.ImmutablesStyles.StagedBuilderStyle;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.time.Clock;
import com.palantir.conjure.java.api.config.service.ServicesConfigBlock;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.api.config.service.UserAgents;
import com.palantir.dialogue.clients.DialogueClients;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockServerConfigs;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.client.LockRefreshingLockService;
import com.palantir.lock.client.TimeLockClient;
import com.palantir.lock.client.metrics.TimeLockFeedbackBackgroundTask;
import com.palantir.lock.impl.LockServiceImpl;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.DoNotLog;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.immutables.value.Value;

@DoNotLog
@Value.Immutable
@StagedBuilderStyle
public abstract class TransactionManagers {
    private static final SafeLogger log = SafeLoggerFactory.get(TransactionManagers.class);
    public static final LockClient LOCK_CLIENT = LockClient.of("atlas instance");

    abstract AtlasDbConfig config();

    abstract Optional<Refreshable<Optional<AtlasDbRuntimeConfig>>> runtimeConfig();

    /**
     * Use {@link #runtimeConfig} instead.
     */
    abstract Optional<Supplier<Optional<AtlasDbRuntimeConfig>>> runtimeConfigSupplier();

    abstract Set<Schema> schemas();

    /**
     * This method is not used and should not be overriden from the default. It only exists because deleting
     * it would be an ABI break.
     */
    @Value.Default
    Consumer<Object> registrar() {
        return resource -> {};
    }

    @Value.Default
    LockServerOptions lockServerOptions() {
        return LockServerConfigs.DEFAULT;
    }

    @Value.Default
    boolean allowHiddenTableAccess() {
        return false;
    }

    @Value.Default
    boolean validateLocksOnReads() {
        return true;
    }

    @Value.Default
    boolean lockImmutableTsOnReadOnlyTransactions() {
        return false;
    }

    @Value.Default
    boolean allSafeForLogging() {
        return false;
    }

    @Value.Default
    BiFunction<TimelockService, TimestampManagementService, TimeLockClient> defaultTimelockClientFactory() {
        return (service, _management) -> TimeLockClient.createDefault(service);
    }

    abstract Optional<LockAndTimestampServiceFactory> lockAndTimestampServiceFactory();

    abstract UserAgent userAgent();

    abstract Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders();

    /**
     * Please use {@link #globalTaggedMetricRegistry()} instead. The publishing of metrics to the external metrics
     * registry has ceased; they are all published to the tagged registry instead.
     */
    abstract MetricRegistry globalMetricsRegistry();

    abstract TaggedMetricRegistry globalTaggedMetricRegistry();

    /**
     * The callback Runnable will be run when the TransactionManager is successfully initialized. The
     * TransactionManager will stay uninitialized and continue to throw for all other purposes until the callback
     * returns at which point it will become initialized. If asynchronous initialization is disabled, the callback will
     * be run just before the TM is returned.
     * <p>
     * Note that if the callback blocks forever, the TransactionManager will never become initialized, and calling its
     * close() method will block forever as well. If the callback init() fails, and its cleanup() method throws,
     * the TransactionManager will not become initialized and it will be closed.
     */
    @Value.Default
    Callback<TransactionManager> asyncInitializationCallback() {
        return Callback.noOp();
    }

    // TODO(fdesouza): Remove this once PDS-95791 is resolved.
    abstract Optional<LockDiagnosticComponents> lockDiagnosticComponents();

    @Value.Default
    LockWatchingCache lockWatchingCache() {
        return NoOpLockWatchingCache.INSTANCE;
    }

    @Value.Default
    MetricsFilterEvaluationContext metricsFilterEvaluationContext() {
        return DefaultMetricsFilterEvaluationContext.createDefault();
    }

    /**
     * If set, a {@link com.palantir.dialogue.clients.DialogueClients.ReloadingFactory} that a
     * {@link com.palantir.atlasdb.transaction.api.TransactionManager} based on this configuration should use.
     * This may be useful for ensuring that connection pools are shared between multiple TransactionManagers in
     * the same JVM.
     * We intend this to be used in exactly one place - do not use this without discussing with the AtlasDB team.
     */
    @Value.Default
    DialogueClients.ReloadingFactory reloadingFactory() {
        return newMinimalDialogueFactory();
    }

    public static ImmutableTransactionManagers.ConfigBuildStage builder() {
        return ImmutableTransactionManagers.builder();
    }

    @SuppressWarnings("immutables:incompat")
    @VisibleForTesting
    static Consumer<Runnable> runAsync = task -> {
        Thread thread = new Thread(task);
        thread.setDaemon(true);
        thread.start();
    };

    @Value.Check
    protected void check() {
        Preconditions.checkState(
                !(runtimeConfigSupplier().isPresent() && runtimeConfig().isPresent()),
                "Cannot provide both Refreshable and Supplier of runtime config");
    }

    /**
     * Accepts a single {@link Schema}.
     *
     * @see TransactionManagers#createInMemory(Set)
     */
    public static TransactionManager createInMemory(Schema schema) {
        return createInMemory(ImmutableSet.of(schema));
    }

    /**
     * Create a {@link TransactionManager} backed by an
     * {@link com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService}. This should be used for testing
     * purposes only.
     */
    public static TransactionManager createInMemory(Set<Schema> schemas) {
        return createInMemory(schemas, Optional.empty());
    }

    public static TransactionManager createInMemory(Schema schema, LockAndTimestampServiceFactory factory) {
        return createInMemory(Set.of(schema), Optional.of(factory));
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static TransactionManager createInMemory(
            Set<Schema> schemas, Optional<LockAndTimestampServiceFactory> maybeFactory) {
        AtlasDbConfig config = ImmutableAtlasDbConfig.builder()
                .keyValueService(new InMemoryAtlasDbConfig())
                .collectThreadDumpOnTimestampServiceInit(false)
                .build();
        return builder()
                .config(config)
                .userAgent(AtlasDbRemotingConstants.DEFAULT_USER_AGENT)
                .globalMetricsRegistry(new MetricRegistry())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .lockAndTimestampServiceFactory(maybeFactory)
                .addAllSchemas(schemas)
                .build()
                .serializable();
    }

    @JsonIgnore
    @Value.Derived
    public TransactionManager serializable() {
        List<AutoCloseable> closeables = new ArrayList<>();

        try {
            return serializableInternal(closeables);
        } catch (Throwable throwable) {
            List<String> closeablesClasses = closeables.stream()
                    .map(autoCloseable -> autoCloseable.getClass().toString())
                    .collect(Collectors.toList());

            log.warn(
                    "Exception thrown when creating transaction manager. " + "Closing previously opened resources: {}",
                    SafeArg.of("classes", closeablesClasses.toString()),
                    throwable);

            closeables.forEach(autoCloseable -> {
                try {
                    autoCloseable.close();
                } catch (Exception ex) {
                    log.info(
                            "Error closing {}",
                            SafeArg.of("class", autoCloseable.getClass().toString()),
                            ex);
                }
            });
            throw throwable;
        }
    }

    @SuppressWarnings("MethodLength")
    private TransactionManager serializableInternal(@Output List<AutoCloseable> closeables) {
        MetricsManager metricsManager = setUpMetricsAndGetMetricsManager();

        AtlasDbRuntimeConfigRefreshable runtimeConfigRefreshable =
                initializeCloseable(() -> AtlasDbRuntimeConfigRefreshable.create(this), closeables);

        Refreshable<AtlasDbRuntimeConfig> runtime = runtimeConfigRefreshable.config();

        Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask =
                getTimeLockFeedbackBackgroundTask(metricsManager, closeables, config(), runtime);

        FreshTimestampSupplierAdapter adapter = new FreshTimestampSupplierAdapter();
        KeyValueServiceConfig installConfig = config().keyValueService();
        ServiceDiscoveringAtlasSupplier atlasFactory = new ServiceDiscoveringAtlasSupplier(
                metricsManager,
                installConfig,
                runtime.map(AtlasDbRuntimeConfig::keyValueService),
                config().namespace(),
                Optional.empty(),
                config().initializeAsync(),
                config().collectThreadDumpOnTimestampServiceInit(),
                adapter);
        DerivedSnapshotConfig derivedSnapshotConfig = atlasFactory.getDerivedSnapshotConfig();

        LockRequest.setDefaultLockTimeout(
                SimpleTimeDuration.of(config().getDefaultLockTimeoutSeconds(), TimeUnit.SECONDS));

        LockAndTimestampServiceFactory factory = lockAndTimestampServiceFactory()
                .orElseGet(() -> new DefaultLockAndTimestampServiceFactory(
                        metricsManager,
                        config(),
                        runtime,
                        () -> LockServiceImpl.create(lockServerOptions()),
                        atlasFactory::getManagedTimestampService,
                        atlasFactory.getTimestampStoreInvalidator(),
                        userAgent(),
                        lockDiagnosticComponents(),
                        reloadingFactory(),
                        timeLockFeedbackBackgroundTask,
                        timelockRequestBatcherProviders(),
                        schemas(),
                        defaultTimelockClientFactory()));
        LockAndTimestampServices lockAndTimestampServices = factory.createLockAndTimestampServices();
        adapter.setTimestampService(lockAndTimestampServices.managedTimestampService());

        KvsProfilingLogger.setSlowLogThresholdMillis(config().getKvsSlowLogThresholdMillis());

        KeyValueService internalKeyValueService = initializeCloseable(
                () -> {
                    KeyValueService kvs = atlasFactory.getKeyValueService(); // CassandraKeyValueServiceImpl
                    kvs = ProfilingKeyValueService.create(kvs); // Profiling, we think ok
                    kvs = new SafeTableClearerKeyValueService(
                            lockAndTimestampServices.timelock()::getImmutableTimestamp, kvs); // Autodelegate, ok

                    if (!targetedSweepIsEnabled(runtime)) {
                        log.warn("Targeted sweep is not enabled, which means cruft will accumulate in your database if"
                                + " there are overwrites.");
                    }

                    kvs = TracingKeyValueService.create(kvs); // ok
                    kvs = AtlasDbMetrics.instrumentTimed(
                            metricsManager.getRegistry(),
                            KeyValueService.class,
                            kvs,
                            MetricRegistry.name(KeyValueService.class)); // ok?
                    return ValidatingQueryRewritingKeyValueService.create(kvs); // ok
                },
                closeables);
        KeyValueServiceManager keyValueServiceManager = new DefaultKeyValueServiceManager(metricsManager, adapter);
        TransactionKeyValueServiceManager transactionKeyValueServiceManager = initializeCloseable(
                () -> {
                    TransactionKeyValueServiceManager tkvsm;
                    if (config().transactionKeyValueService().isPresent()) {
                        TransactionKeyValueServiceManagerFactory<?> tkvsmfFactory =
                                AtlasDbServiceDiscovery.createTransactionKeyValueServiceManagerFactoryOfCorrectType(
                                        config().transactionKeyValueService().get());
                        tkvsm = createTransactionKeyValueServiceManager(
                                tkvsmfFactory,
                                metricsManager,
                                lockAndTimestampServices,
                                internalKeyValueService,
                                keyValueServiceManager,
                                config().transactionKeyValueService().get(),
                                runtimeConfig().get().map(optionalConfig -> optionalConfig
                                        .get()
                                        .transactionKeyValueService()
                                        .get()),
                                config().initializeAsync());
                    } else {
                        tkvsm = new DelegatingTransactionKeyValueServiceManager(internalKeyValueService);
                    }
                    return tkvsm;
                },
                closeables);

        TransactionManagersInitializer initializer = TransactionManagersInitializer.createInitialTables(
                internalKeyValueService,
                transactionKeyValueServiceManager,
                schemas(),
                config().initializeAsync(),
                allSafeForLogging());
        CleanupFollower follower = CleanupFollower.create(schemas());

        CoordinationService<InternalSchemaMetadata> coordinationService =
                getSchemaMetadataCoordinationService(metricsManager, lockAndTimestampServices, internalKeyValueService);

        TargetedSweeper targetedSweeper = uninitializedTargetedSweeper(
                internalKeyValueService,
                metricsManager,
                config().targetedSweep(),
                follower,
                runtime.map(AtlasDbRuntimeConfig::targetedSweep),
                coordinationService);

        TransactionSchemaManager transactionSchemaManager = new TransactionSchemaManager(coordinationService);

        TransactionKnowledgeComponents knowledge = TransactionKnowledgeComponents.create(
                internalKeyValueService,
                metricsManager.getTaggedRegistry(),
                config().internalSchema(),
                targetedSweeper::isInitialized);

        TransactionComponents components = createTransactionComponents(
                closeables, metricsManager, knowledge, transactionSchemaManager, internalKeyValueService, runtime);

        TransactionService transactionService = components.transactionService();
        ConflictDetectionManager conflictManager = ConflictDetectionManagers.create(internalKeyValueService);
        SweepStrategyManager sweepStrategyManager = SweepStrategyManagers.createDefault(internalKeyValueService);

        Cleaner cleaner = initializeCloseable(
                () -> new DefaultCleanerBuilder(
                                internalKeyValueService,
                                lockAndTimestampServices.timelock(),
                                ImmutableList.of(follower),
                                transactionService,
                                metricsManager)
                        .setBackgroundScrubAggressively(config().backgroundScrubAggressively())
                        .setBackgroundScrubBatchSize(config().getBackgroundScrubBatchSize())
                        .setBackgroundScrubFrequencyMillis(config().getBackgroundScrubFrequencyMillis())
                        .setBackgroundScrubThreads(config().getBackgroundScrubThreads())
                        .setPunchIntervalMillis(config().getPunchIntervalMillis())
                        .setTransactionReadTimeout(config().getTransactionReadTimeoutMillis())
                        .setInitializeAsync(config().initializeAsync())
                        .buildCleaner(),
                closeables);

        MultiTableSweepQueueWriter targetedSweep = initializeCloseable(() -> targetedSweeper, closeables);

        Supplier<TransactionConfig> transactionConfigSupplier =
                runtime.map(AtlasDbRuntimeConfig::transaction).map(this::withConsolidatedGrabImmutableTsLockFlag);

        TimestampCache timestampCache = config().timestampCache()
                .orElseGet(() -> new DefaultTimestampCache(
                        metricsManager.getRegistry(), () -> runtime.get().getTimestampCacheSize()));

        ConflictTracer conflictTracer = lockDiagnosticComponents()
                .map(LockDiagnosticComponents::clientLockDiagnosticCollector)
                .<ConflictTracer>map(Function.identity())
                .orElse(ConflictTracer.NO_OP);

        Callback<TransactionManager> callbacks = new Callback.CallChain<>(List.of(
                timelockConsistencyCheckCallback(
                        config(), runtime.get(), lockAndTimestampServices, internalKeyValueService),
                targetedSweep.singleAttemptCallback(),
                asyncInitializationCallback(),
                createClearsTable(internalKeyValueService)));

        TransactionManager transactionManager = initializeCloseable(
                () -> SerializableTransactionManager.createInstrumented(
                        metricsManager,
                        transactionKeyValueServiceManager,
                        lockAndTimestampServices.timelock(),
                        lockAndTimestampServices.lockWatcher(),
                        lockAndTimestampServices.managedTimestampService(),
                        lockAndTimestampServices.lock(),
                        transactionService,
                        () -> AtlasDbConstraintCheckingMode.FULL_CONSTRAINT_CHECKING_THROWS_EXCEPTIONS,
                        conflictManager,
                        sweepStrategyManager,
                        cleaner,
                        () -> areTransactionManagerInitializationPrerequisitesSatisfied(
                                initializer, internalKeyValueService, lockAndTimestampServices),
                        allowHiddenTableAccess(),
                        derivedSnapshotConfig.concurrentGetRangesThreadPoolSize(),
                        derivedSnapshotConfig.defaultGetRangesConcurrency(),
                        config().initializeAsync(),
                        timestampCache,
                        targetedSweep,
                        callbacks,
                        validateLocksOnReads(),
                        transactionConfigSupplier,
                        conflictTracer,
                        metricsFilterEvaluationContext(),
                        installConfig.sharedResourcesConfig().map(SharedResourcesConfig::sharedGetRangesPoolSize),
                        knowledge),
                closeables);

        transactionManager.registerClosingCallback(runtimeConfigRefreshable::close);

        timeLockFeedbackBackgroundTask.ifPresent(task -> transactionManager.registerClosingCallback(task::close));

        lockAndTimestampServices.resources().forEach(transactionManager::registerClosingCallback);
        transactionManager.registerClosingCallback(transactionService::close);
        components
                .schemaInstaller()
                .ifPresent(installer -> transactionManager.registerClosingCallback(installer::close));
        transactionManager.registerClosingCallback(targetedSweep::close);

        initializeCloseable(
                initializeCompactBackgroundProcess(
                        metricsManager,
                        lockAndTimestampServices,
                        internalKeyValueService,
                        transactionManager,
                        runtime.map(AtlasDbRuntimeConfig::compact)),
                closeables);

        log.info("Successfully created, and now returning a transaction manager: this may not be fully initialised.");
        return transactionManager;
    }

    private <T> TransactionKeyValueServiceManager createTransactionKeyValueServiceManager(
            TransactionKeyValueServiceManagerFactory<T> factory,
            MetricsManager metricsManager,
            LockAndTimestampServices lockAndTimestampServices,
            KeyValueService internalTablesKeyValueService,
            KeyValueServiceManager keyValueServiceManager,
            TransactionKeyValueServiceConfig config,
            Refreshable<TransactionKeyValueServiceRuntimeConfig> runtimeConfigRefreshable,
            boolean initializeAsync) {
        CoordinationService<T> coordinationService =
                CoordinationServices.createTransactionKeyValueServiceManagerCoordinator(
                        factory.coordinationValueClass(),
                        internalTablesKeyValueService,
                        lockAndTimestampServices.managedTimestampService(),
                        metricsManager,
                        config().initializeAsync());
        return factory.create(
                config().namespace()
                        .orElseThrow(() -> new SafeIllegalArgumentException(
                                "Namespace must be set when using transactionKeyValueService")),
                reloadingFactory()
                        .withUserAgent(userAgent().addAgent(AtlasDbRemotingConstants.ATLASDB_HTTP_CLIENT_AGENT)),
                metricsManager,
                coordinationService,
                keyValueServiceManager,
                config,
                runtimeConfigRefreshable,
                initializeAsync);
    }

    private MetricsManager setUpMetricsAndGetMetricsManager() {
        MetricRegistry internalAtlasDbMetrics = new MetricRegistry();
        TaggedMetricRegistry internalTaggedAtlasDbMetrics = new DefaultTaggedMetricRegistry();
        MetricsManager metricsManager = MetricsManagers.of(
                internalAtlasDbMetrics,
                internalTaggedAtlasDbMetrics,
                runtimeConfig()
                        .map(runtimeConfigRefreshable -> runtimeConfigRefreshable.map(maybeRuntime -> maybeRuntime
                                .map(AtlasDbRuntimeConfig::enableMetricFiltering)
                                .orElse(true)))
                        .orElseGet(() -> Refreshable.only(true)));
        globalTaggedMetricRegistry()
                .addMetrics(
                        AtlasDbMetricNames.LIBRARY_ORIGIN_TAG,
                        AtlasDbMetricNames.LIBRARY_ORIGIN_VALUE,
                        metricsManager.getPublishableMetrics());
        return metricsManager;
    }

    private Optional<TimeLockFeedbackBackgroundTask> getTimeLockFeedbackBackgroundTask(
            MetricsManager metricsManager,
            @Output List<AutoCloseable> closeables,
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfig) {
        if (isUsingTimeLock(config, runtimeConfig.current()) && shouldGiveFeedbackToServer(config)) {
            Refreshable<List<TimeLockClientFeedbackService>> refreshableTimeLockClientFeedbackServices =
                    getTimeLockClientFeedbackServices(config, runtimeConfig, userAgent(), reloadingFactory());
            Refreshable<Integer> timelockNodeCountSupplier =
                    DefaultLockAndTimestampServiceFactory.getServerListConfigSupplierForTimeLock(config, runtimeConfig)
                            .map(ServerListConfig::servers)
                            .map(Set::size);

            return Optional.of(initializeCloseable(
                    () -> TimeLockFeedbackBackgroundTask.create(
                            metricsManager.getTaggedRegistry(),
                            AtlasDbVersion::readVersion,
                            serviceName(),
                            refreshableTimeLockClientFeedbackServices,
                            timelockNodeCountSupplier,
                            namespace()),
                    closeables));
        }
        return Optional.empty();
    }

    @VisibleForTesting
    static Refreshable<List<TimeLockClientFeedbackService>> getTimeLockClientFeedbackServices(
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfig,
            UserAgent userAgent,
            DialogueClients.ReloadingFactory reloadingFactory) {
        Refreshable<ServerListConfig> serverListConfigSupplier =
                DefaultLockAndTimestampServiceFactory.getServerListConfigSupplierForTimeLock(config, runtimeConfig);

        BroadcastDialogueClientFactory broadcastDialogueClientFactory = BroadcastDialogueClientFactory.create(
                reloadingFactory,
                serverListConfigSupplier,
                userAgent,
                AuxiliaryRemotingParameters.builder()
                        .shouldRetry(true)
                        .userAgent(userAgent)
                        .shouldLimitPayload(true)
                        .build());

        return broadcastDialogueClientFactory.getSingleNodeProxies(TimeLockClientFeedbackService.class, true);
    }

    private static DialogueClients.ReloadingFactory newMinimalDialogueFactory() {
        return DialogueClients.create(
                        Refreshable.only(ServicesConfigBlock.builder().build()))
                .withBlockingExecutor(PTExecutors.newCachedThreadPool("atlas-dialogue-blocking"));
    }

    abstract Optional<String> serviceIdentifierOverride();

    @VisibleForTesting
    @Value.Derived
    String serviceName() {
        return serviceIdentifierOverride().orElseGet(this::namespace);
    }

    @Value.Derived
    String namespace() {
        return Stream.of(
                        config().namespace(),
                        config().timelock().flatMap(TimeLockClientConfig::client),
                        config().keyValueService().namespace())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElse("UNKNOWN");
    }

    private static Callback<TransactionManager> createClearsTable(KeyValueService internalKeyValueService) {
        TableReference clearsTableRef =
                TargetedSweepTableFactory.of().getTableClearsTable(null).getTableRef();
        byte[] clearsTableMetadata = TargetedSweepSchema.INSTANCE
                .getLatestSchema()
                .getAllTablesAndIndexMetadata()
                .get(clearsTableRef)
                .persistToBytes();
        return LambdaCallback.of(tm -> internalKeyValueService.createTable(clearsTableRef, clearsTableMetadata));
    }

    @VisibleForTesting
    TransactionConfig withConsolidatedGrabImmutableTsLockFlag(TransactionConfig transactionConfig) {
        return ImmutableTransactionConfig.copyOf(transactionConfig)
                .withLockImmutableTsOnReadOnlyTransactions(lockImmutableTsOnReadOnlyTransactions()
                        || transactionConfig.lockImmutableTsOnReadOnlyTransactions());
    }

    private static boolean targetedSweepIsEnabled(Supplier<AtlasDbRuntimeConfig> runtime) {
        return runtime.get().targetedSweep().enabled();
    }

    private TransactionComponents createTransactionComponents(
            @Output List<AutoCloseable> closeables,
            MetricsManager metricsManager,
            TransactionKnowledgeComponents knowledgeCache,
            TransactionSchemaManager transactionSchemaManager,
            KeyValueService keyValueService,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier) {
        TransactionService transactionService = initializeCloseable(
                () -> AtlasDbMetrics.instrumentTimed(
                        metricsManager.getRegistry(),
                        TransactionService.class,
                        TransactionServices.createTransactionService(
                                keyValueService,
                                transactionSchemaManager,
                                knowledgeCache,
                                metricsManager.getTaggedRegistry(),
                                () -> runtimeConfigSupplier
                                        .get()
                                        .internalSchema()
                                        .acceptStagingReadsOnVersionThree())),
                closeables);
        Optional<TransactionSchemaInstaller> schemaInstaller = getTransactionSchemaInstallerIfSupported(
                closeables, keyValueService, runtimeConfigSupplier, transactionSchemaManager);
        return ImmutableTransactionComponents.builder()
                .transactionService(transactionService)
                .schemaInstaller(schemaInstaller)
                .build();
    }

    private static Optional<TransactionSchemaInstaller> getTransactionSchemaInstallerIfSupported(
            @Output List<AutoCloseable> closeables,
            KeyValueService keyValueService,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            TransactionSchemaManager transactionSchemaManager) {
        CheckAndSetCompatibility compatibility = keyValueService.getCheckAndSetCompatibility();
        if (compatibility.supportsCheckAndSetOperations() && compatibility.supportsDetailOnFailure()) {
            return Optional.of(
                    initializeTransactionSchemaInstaller(closeables, runtimeConfigSupplier, transactionSchemaManager));
        }
        runtimeConfigSupplier
                .get()
                .internalSchema()
                .targetTransactionsSchemaVersion()
                .filter(version -> version != TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .ifPresent(version -> log.warn(
                        "This service seems like it has been configured to use transaction schema version {},"
                                + " which isn't supported as your KVS doesn't support details on CAS failures"
                                + " (typically Postgres or Oracle). We will remain with transactions1.",
                        SafeArg.of("configuredTransactionSchemaVersion", version)));
        return Optional.empty();
    }

    private static TransactionSchemaInstaller initializeTransactionSchemaInstaller(
            @Output List<AutoCloseable> closeables,
            Supplier<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            TransactionSchemaManager transactionSchemaManager) {
        return initializeCloseable(
                () -> TransactionSchemaInstaller.createStarted(
                        transactionSchemaManager,
                        () -> runtimeConfigSupplier.get().internalSchema().targetTransactionsSchemaVersion()),
                closeables);
    }

    private CoordinationService<InternalSchemaMetadata> getSchemaMetadataCoordinationService(
            MetricsManager metricsManager,
            LockAndTimestampServices lockAndTimestampServices,
            KeyValueService keyValueService) {
        CoordinationService<InternalSchemaMetadata> metadataCoordinationService = CoordinationServices.createDefault(
                keyValueService,
                lockAndTimestampServices.managedTimestampService(),
                metricsManager,
                config().initializeAsync());
        MetadataCoordinationServiceMetrics.registerMetrics(
                metricsManager, metadataCoordinationService, lockAndTimestampServices.managedTimestampService());
        return metadataCoordinationService;
    }

    private static Optional<BackgroundCompactor> initializeCompactBackgroundProcess(
            MetricsManager metricsManager,
            LockAndTimestampServices lockAndTimestampServices,
            KeyValueService keyValueService,
            TransactionManager transactionManager,
            Supplier<CompactorConfig> compactorConfigSupplier) {
        Optional<BackgroundCompactor> backgroundCompactorOptional = BackgroundCompactor.createAndRun(
                metricsManager,
                transactionManager,
                keyValueService,
                lockAndTimestampServices.lock(),
                compactorConfigSupplier);

        backgroundCompactorOptional.ifPresent(
                backgroundCompactor -> transactionManager.registerClosingCallback(backgroundCompactor::close));

        return backgroundCompactorOptional;
    }

    private static <T extends AutoCloseable> T initializeCloseable(
            Supplier<T> closeableSupplier, @Output List<AutoCloseable> closeables) {
        T ret = closeableSupplier.get();
        closeables.add(ret);
        return ret;
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private static <T extends AutoCloseable> Optional<T> initializeCloseable(
            Optional<T> closeableOptional, @Output List<AutoCloseable> closeables) {
        closeableOptional.ifPresent(closeables::add);
        return closeableOptional;
    }

    private static boolean areTransactionManagerInitializationPrerequisitesSatisfied(
            AsyncInitializer initializer,
            KeyValueService internalKeyValueService,
            LockAndTimestampServices lockAndTimestampServices) {
        return initializer.isInitialized()
                && internalKeyValueService.isInitialized()
                && timeLockMigrationCompleteIfNeeded(lockAndTimestampServices);
    }

    @VisibleForTesting
    static boolean timeLockMigrationCompleteIfNeeded(LockAndTimestampServices lockAndTimestampServices) {
        return lockAndTimestampServices
                .migrator()
                .map(AsyncInitializer::isInitialized)
                .orElse(true);
    }

    private static Callback<TransactionManager> timelockConsistencyCheckCallback(
            AtlasDbConfig atlasDbConfig,
            AtlasDbRuntimeConfig initialRuntimeConfig,
            LockAndTimestampServices lockAndTimestampServices,
            KeyValueService internalKeyValueService) {
        // Only do the consistency check if we're using TimeLock.
        // This avoids a bootstrapping problem with leader-block services without async initialisation,
        // where you need a working timestamp service to check consistency, you need to check consistency
        // before you can return a TM, you need to return a TM to listen on ports, and you need to listen on
        // ports in order to get a working timestamp service.
        if (isUsingTimeLock(atlasDbConfig, initialRuntimeConfig)) {
            ToLongFunction<TransactionManager> conservativeBoundSupplier = txnManager -> {
                Clock clock = GlobalClock.create(txnManager.getTimelockService());
                return KeyValueServicePuncherStore.get(internalKeyValueService, clock.getTimeMillis());
            };
            return ConsistencyCheckRunner.create(ImmutableTimestampCorroborationConsistencyCheck.builder()
                    .conservativeBound(conservativeBoundSupplier)
                    .freshTimestampSource(
                            unused -> lockAndTimestampServices.timelock().getFreshTimestamp())
                    .build());
        }
        return Callback.noOp();
    }

    // TODO(gs): wrap install and runtime config objects to avoid passing them both in?
    static boolean isUsingTimeLock(AtlasDbConfig atlasDbConfig, AtlasDbRuntimeConfig runtimeConfig) {
        return atlasDbConfig.timelock().isPresent()
                || runtimeConfig.timelockRuntime().isPresent();
    }

    private boolean shouldGiveFeedbackToServer(AtlasDbConfig config) {
        return config.timelock()
                .map(TimeLockClientConfig::shouldGiveFeedbackToTimeLockServer)
                .orElse(false);
    }

    /**
     * This method should not be used directly. It remains here to support the AtlasDB-Dagger module and the CLIs, but
     * may be removed at some point in the future.
     *
     * @deprecated Not intended for public use outside of the AtlasDB CLIs
     */
    @Deprecated
    public static LockAndTimestampServices createLockAndTimestampServicesForCli(
            MetricsManager metricsManager,
            AtlasDbConfig config,
            Refreshable<AtlasDbRuntimeConfig> runtimeConfigSupplier,
            Consumer<Object> registrar,
            Supplier<LockService> lock,
            Supplier<ManagedTimestampService> time,
            TimestampStoreInvalidator invalidator,
            String userAgent) {
        LockAndTimestampServices lockAndTimestampServices =
                DefaultLockAndTimestampServiceFactory.createRawInstrumentedServices(
                        metricsManager,
                        config,
                        runtimeConfigSupplier,
                        lock,
                        time,
                        invalidator,
                        UserAgents.tryParse(userAgent),
                        Optional.empty(),
                        newMinimalDialogueFactory(),
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableSet.of());
        TimeLockClient timeLockClient = TimeLockClient.withSynchronousUnlocker(lockAndTimestampServices.timelock());
        return ImmutableLockAndTimestampServices.builder()
                .from(lockAndTimestampServices)
                .timelock(timeLockClient)
                .lock(LockRefreshingLockService.create(lockAndTimestampServices.lock()))
                .addResources(timeLockClient::close)
                .build();
    }

    private static TargetedSweeper uninitializedTargetedSweeper(
            KeyValueService kvs,
            MetricsManager metricsManager,
            TargetedSweepInstallConfig install,
            Follower follower,
            Supplier<TargetedSweepRuntimeConfig> runtime,
            CoordinationService<InternalSchemaMetadata> coordinationService) {
        CoordinationAwareKnownAbandonedTransactionsStore abandonedTxnStore =
                new CoordinationAwareKnownAbandonedTransactionsStore(
                        coordinationService, new AbandonedTimestampStoreImpl(kvs));
        return TargetedSweeper.createUninitialized(
                metricsManager,
                runtime,
                install,
                ImmutableList.of(follower),
                abandonedTxnStore::addAbandonedTimestamps,
                kvs);
    }

    @Value.Immutable
    public interface TransactionComponents {
        TransactionService transactionService();

        Optional<TransactionSchemaInstaller> schemaInstaller();
    }
}
