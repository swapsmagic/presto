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
package com.facebook.presto.queue.manager;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.drift.transport.netty.server.DriftNettyServerModule;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;

import javax.inject.Singleton;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.drift.server.guice.DriftServerBinder.driftServerBinder;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RegionalQueueModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.install(new DriftNettyServerModule());
        driftServerBinder(binder).bindService(RegionalQueueServer.class);
        binder.bind(QueueManager.class).to(BasicQueueManager.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public ListeningExecutorService createRegionalQueueManagerExecutor()
    {
        ExecutorService executor = new ThreadPoolExecutor(
                0,
                10,
                60,
                SECONDS,
                new LinkedBlockingQueue<>(),
                daemonThreadsNamed("resource-manager-executor-%s"));
        return listeningDecorator(executor);
    }
}
