/*
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
package com.facebook.presto.server;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.airlift.discovery.server.EmbeddedDiscoveryModule;
import com.facebook.presto.dispatcher.DispatchExecutor;
import com.facebook.presto.dispatcher.DispatchQueryFactory;
import com.facebook.presto.dispatcher.FailedDispatchQueryFactory;
import com.facebook.presto.dispatcher.NoOpQueryManager;
import com.facebook.presto.dispatcher.RMDispatchManager;
import com.facebook.presto.dispatcher.RMDispatchQueryFactory;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.event.QueryMonitorConfig;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.NodeResourceStatusConfig;
import com.facebook.presto.execution.QueryIdGenerator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.resourceGroups.InternalResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.LegacyResourceGroupConfigurationManager;
import com.facebook.presto.execution.resourceGroups.NoOpResourceGroupManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.failureDetector.FailureDetectorModule;
import com.facebook.presto.memory.ClusterMemoryManager;
import com.facebook.presto.memory.ForMemoryManager;
import com.facebook.presto.memory.LowMemoryKiller;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NoneLowMemoryKiller;
import com.facebook.presto.memory.TotalReservationLowMemoryKiller;
import com.facebook.presto.memory.TotalReservationOnBlockedNodesLowMemoryKiller;
import com.facebook.presto.operator.OperatorInfo;
import com.facebook.presto.resourcemanager.DistributedClusterStatsResource;
import com.facebook.presto.resourcemanager.DistributedQueryInfoResource;
import com.facebook.presto.resourcemanager.DistributedQueryResource;
import com.facebook.presto.resourcemanager.DistributedResourceGroupInfoResource;
import com.facebook.presto.resourcemanager.DistributedTaskInfoResource;
import com.facebook.presto.resourcemanager.ForResourceManager;
import com.facebook.presto.resourcemanager.QueuedStatementResource;
import com.facebook.presto.resourcemanager.RaftConfig;
import com.facebook.presto.resourcemanager.RatisServer;
import com.facebook.presto.resourcemanager.ResourceManagerClusterStateProvider;
import com.facebook.presto.resourcemanager.ResourceManagerProxy;
import com.facebook.presto.resourcemanager.ResourceManagerServer;
import com.facebook.presto.server.protocol.LocalQueryProvider;
import com.facebook.presto.server.protocol.QueryBlockingRateLimiter;
import com.facebook.presto.server.protocol.RetryCircuitBreaker;
import com.facebook.presto.spi.memory.ClusterMemoryPoolManager;
import com.facebook.presto.transaction.NoOpTransactionManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.configuration.ConditionalModule.installModuleIf;
import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static com.facebook.airlift.http.client.HttpClientBinder.httpClientBinder;
import static com.facebook.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static com.facebook.airlift.json.JsonBinder.jsonBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.airlift.json.smile.SmileCodecBinder.smileCodecBinder;
import static com.facebook.drift.server.guice.DriftServerBinder.driftServerBinder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ResourceManagerModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        // discovery server
        install(installModuleIf(EmbeddedDiscoveryConfig.class, EmbeddedDiscoveryConfig::isEnabled, new EmbeddedDiscoveryModule()));

        // presto coordinator announcement
        discoveryBinder(binder).bindHttpAnnouncement("presto-resource-manager");

        // statement resource
        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);

        // resource for serving static content
        jaxrsBinder(binder).bind(WebUiResource.class);

        // failure detector
        binder.install(new FailureDetectorModule());
        jaxrsBinder(binder).bind(NodeResource.class);
        jaxrsBinder(binder).bind(WorkerResource.class);
        httpClientBinder(binder).bindHttpClient("workerInfo", ForWorkerInfo.class);

        // TODO: decouple query-level configuration that is not needed for Resource Manager
        binder.bind(QueryManager.class).to(NoOpQueryManager.class).in(Scopes.SINGLETON);
        jaxrsBinder(binder).bind(DistributedResourceGroupInfoResource.class);
        binder.bind(QueryIdGenerator.class).in(Scopes.SINGLETON);

        binder.bind(SessionSupplier.class).to(QuerySessionSupplier.class).in(Scopes.SINGLETON);

        jsonCodecBinder(binder).bindJsonCodec(QueryInfo.class);
        smileCodecBinder(binder).bindSmileCodec(QueryInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(BasicQueryInfo.class);
        smileCodecBinder(binder).bindSmileCodec(BasicQueryInfo.class);
        jsonCodecBinder(binder).bindListJsonCodec(QueryStateInfo.class);
        jsonCodecBinder(binder).bindJsonCodec(ResourceGroupInfo.class);

        binder.bind(TransactionManager.class).to(NoOpTransactionManager.class);

        binder.bind(ResourceManagerClusterStateProvider.class).in(Scopes.SINGLETON);
        driftServerBinder(binder).bindService(ResourceManagerServer.class);

        binder.bind(NodeResourceStatusProvider.class).toInstance(() -> true);

        jaxrsBinder(binder).bind(DistributedQueryResource.class);
        jaxrsBinder(binder).bind(DistributedQueryInfoResource.class);
        jaxrsBinder(binder).bind(DistributedClusterStatsResource.class);
        jaxrsBinder(binder).bind(DistributedTaskInfoResource.class);

        //Binding New Queued Endpoint
        jaxrsBinder(binder).bind(QueuedStatementResource.class);

        httpClientBinder(binder).bindHttpClient("resourceManager", ForResourceManager.class);
        binder.bind(ResourceManagerProxy.class).in(Scopes.SINGLETON);
        jsonBinder(binder).addSerializerBinding(Duration.class).to(DurationSerializer.class);
        jsonBinder(binder).addSerializerBinding(DataSize.class).to(DataSizeSerializer.class);

        RaftConfig raftConfig = buildConfigObject(RaftConfig.class);
        if (raftConfig.isEnabled()) {
            binder.bind(RatisServer.class).in(Scopes.SINGLETON);
        }

        ServerConfig serverConfig = buildConfigObject(ServerConfig.class);
        if (serverConfig.isGlobalResourceGroupEnabled()) {
            binder.bind(InternalResourceGroupManager.class).in(Scopes.SINGLETON);
            newExporter(binder).export(InternalResourceGroupManager.class).withGeneratedName();
            binder.bind(ResourceGroupManager.class).to(InternalResourceGroupManager.class);
            binder.bind(LegacyResourceGroupConfigurationManager.class).in(Scopes.SINGLETON);

            binder.bind(ClusterMemoryManager.class).in(Scopes.SINGLETON);
            binder.bind(ClusterMemoryPoolManager.class).to(ClusterMemoryManager.class).in(Scopes.SINGLETON);

            httpClientBinder(binder).bindHttpClient("memoryManager", ForMemoryManager.class)
                    .withTracing()
                    .withConfigDefaults(config -> {
                        config.setRequestTimeout(new Duration(10, SECONDS));
                    });
            bindLowMemoryKiller(MemoryManagerConfig.LowMemoryKillerPolicy.NONE, NoneLowMemoryKiller.class);
            bindLowMemoryKiller(MemoryManagerConfig.LowMemoryKillerPolicy.TOTAL_RESERVATION, TotalReservationLowMemoryKiller.class);
            bindLowMemoryKiller(MemoryManagerConfig.LowMemoryKillerPolicy.TOTAL_RESERVATION_ON_BLOCKED_NODES, TotalReservationOnBlockedNodesLowMemoryKiller.class);
            newExporter(binder).export(ClusterMemoryManager.class).withGeneratedName();

            // dispatcher
            binder.bind(RMDispatchManager.class).in(Scopes.SINGLETON);
            binder.bind(FailedDispatchQueryFactory.class).in(Scopes.SINGLETON);
            binder.bind(DispatchExecutor.class).in(Scopes.SINGLETON);
            newExporter(binder).export(DispatchExecutor.class).withGeneratedName();

            // RM dispatcher
            binder.bind(DispatchQueryFactory.class).to(RMDispatchQueryFactory.class);

            // query monitor
            jsonCodecBinder(binder).bindJsonCodec(OperatorInfo.class);
            configBinder(binder).bindConfig(QueryMonitorConfig.class);
            binder.bind(QueryMonitor.class).in(Scopes.SINGLETON);

            // node monitor
            binder.bind(ClusterSizeMonitor.class).in(Scopes.SINGLETON);

            configBinder(binder).bindConfig(NodeResourceStatusConfig.class);

            binder.bind(LocalQueryProvider.class).in(Scopes.SINGLETON);

            binder.bind(QueryBlockingRateLimiter.class).in(Scopes.SINGLETON);
            newExporter(binder).export(QueryBlockingRateLimiter.class).withGeneratedName();

            binder.bind(RetryCircuitBreaker.class).in(Scopes.SINGLETON);
            newExporter(binder).export(RetryCircuitBreaker.class).withGeneratedName();
        }
        else {
            binder.bind(ResourceGroupManager.class).to(NoOpResourceGroupManager.class);
        }
    }

    private void bindLowMemoryKiller(String name, Class<? extends LowMemoryKiller> clazz)
    {
        install(installModuleIf(
                MemoryManagerConfig.class,
                config -> name.equals(config.getLowMemoryKillerPolicy()),
                binder -> binder.bind(LowMemoryKiller.class).to(clazz).in(Scopes.SINGLETON)));
    }

    @Provides
    @Singleton
    public static ResourceGroupManager<?> getResourceGroupManager(@SuppressWarnings("rawtypes") ResourceGroupManager manager)
    {
        return manager;
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static BoundedExecutor createStatementResponseExecutor(@ForStatementResource ExecutorService coreExecutor, TaskManagerConfig config)
    {
        return new BoundedExecutor(coreExecutor, config.getHttpResponseThreads());
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ExecutorService createStatementResponseCoreExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("statement-response-%s"));
    }

    @Provides
    @Singleton
    @ForStatementResource
    public static ScheduledExecutorService createStatementTimeoutExecutor(TaskManagerConfig config)
    {
        return newScheduledThreadPool(config.getHttpTimeoutThreads(), daemonThreadsNamed("statement-timeout-%s"));
    }
}
