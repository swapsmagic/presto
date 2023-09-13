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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.log.Logger;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;

public class RegionalQueueServerMain implements Runnable
{

    public static void main(String[] args)
    {
        new RegionalQueueServerMain().run();
    }

    @Override
    public void run()
    {
        Logger log = Logger.get(RegionalQueueServerMain.class);

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new RegionalQueueModule()
                );

        Bootstrap app = new Bootstrap(modules.build());

        try {
            Injector injector = app.initialize();

            log.info("======== SERVER STARTED ========");
        }
        catch (Throwable e) {
            log.error(e);
            System.exit(1);
        }
    }
}
