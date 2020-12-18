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
package com.facebook.presto.execution.resourceGroups;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;

import static java.util.Objects.requireNonNull;

@ThriftStruct
public class ResourceGroupRuntimeInfo
{
    private final ResourceGroupId resourceGroupId;
    private final long memoryUsageBytes;
    private final int queuedQueries;
    private final int runningQueries;

    @ThriftConstructor
    public ResourceGroupRuntimeInfo(ResourceGroupId resourceGroupId, long memoryUsageBytes, int queuedQueries, int runningQueries)
    {
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
        this.memoryUsageBytes = memoryUsageBytes;
        this.queuedQueries = queuedQueries;
        this.runningQueries = runningQueries;
    }

    public static Builder builder(ResourceGroupId resourceGroupId)
    {
        return new Builder(resourceGroupId);
    }

    @ThriftField(1)
    public ResourceGroupId getResourceGroupId()
    {
        return resourceGroupId;
    }

    @ThriftField(2)
    public long getMemoryUsageBytes()
    {
        return memoryUsageBytes;
    }

    @ThriftField(3)
    public int getQueuedQueries()
    {
        return queuedQueries;
    }

    @ThriftField(4)
    public int getRunningQueries()
    {
        return runningQueries;
    }

    public static class Builder
    {
        private final ResourceGroupId resourceGroupId;

        private long memoryUsageBytes;
        private int queuedQueries;
        private int runningQueries;

        private Builder(ResourceGroupId resourceGroupId)
        {
            this.resourceGroupId = resourceGroupId;
        }

        public Builder addMemoryUsageBytes(long memoryUsageBytes)
        {
            this.memoryUsageBytes += memoryUsageBytes;
            return this;
        }

        public Builder addQueuedQueries(int queuedQueries)
        {
            this.queuedQueries += queuedQueries;
            return this;
        }

        public Builder addRunningQueries(int runningQueries)
        {
            this.runningQueries = runningQueries;
            return this;
        }

        public ResourceGroupRuntimeInfo build()
        {
            return new ResourceGroupRuntimeInfo(resourceGroupId, memoryUsageBytes, queuedQueries, runningQueries);
        }
    }
}
