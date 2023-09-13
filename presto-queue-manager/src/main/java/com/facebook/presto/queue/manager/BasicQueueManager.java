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

import com.facebook.presto.spi.QueryId;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BasicQueueManager implements QueueManager
{
    List<BasicQueryInfo> queue;
    Map<QueryId, BasicQueryInfo> queryMap;

    @Inject
    public BasicQueueManager()
    {
        queue = new ArrayList<>();
        queryMap = new HashMap<>();
    }
    @Override
    public void submit(BasicQueryInfo basicQueryInfo)
    {
        queue.add(basicQueryInfo);
        queryMap.put(basicQueryInfo.getQueryId(), basicQueryInfo);
    }

    @Override
    public BasicQueryInfo poll()
    {
        if(queue.isEmpty()) {
            return null;
        }
        BasicQueryInfo basicQueryInfo = queue.remove(0);
        queryMap.remove(basicQueryInfo.getQueryId());
        return basicQueryInfo;
    }
}
