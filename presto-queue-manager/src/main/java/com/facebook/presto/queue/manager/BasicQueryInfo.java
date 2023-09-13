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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.common.ErrorType;
import com.facebook.presto.common.resourceGroups.QueryType;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.List;
import java.util.Optional;


import static java.util.Objects.requireNonNull;
import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Lightweight version of QueryInfo. Parts of the web UI depend on the fields
 * being named consistently across these classes.
 */
@ThriftStruct
//@Immutable
public class BasicQueryInfo
{
    private final QueryId queryId;
//    private final SessionRepresentation session;
    private final Optional<ResourceGroupId> resourceGroupId;
//    private final QueryState state;
    private final MemoryPoolId memoryPool;
    private final boolean scheduled;
    private final URI self;
    private final String query;
//    private final String queryHash;
//    private final BasicQueryStats queryStats;
    private final ErrorType errorType;
    private final ErrorCode errorCode;
//    private final ExecutionFailureInfo failureInfo;
    private final Optional<QueryType> queryType;
    private final List<PrestoWarning> warnings;
    private final Optional<String> preparedQuery;

    @ThriftConstructor
    @JsonCreator
    public BasicQueryInfo(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("resourceGroupId") Optional<ResourceGroupId> resourceGroupId,
            @JsonProperty("memoryPool") MemoryPoolId memoryPool,
            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("self") URI self,
            @JsonProperty("query") String query,
            @JsonProperty("errorType") ErrorType errorType,
            @JsonProperty("errorCode") ErrorCode errorCode,
            @JsonProperty("queryType") Optional<QueryType> queryType,
            @JsonProperty("warnings") List<PrestoWarning> warnings,
            @JsonProperty("preparedQuery") Optional<String> preparedQuery)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
//        this.session = requireNonNull(session, "session is null");
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
//        this.state = requireNonNull(state, "state is null");
        this.memoryPool = memoryPool;
        this.errorType = errorType;
        this.errorCode = errorCode;
//        this.failureInfo = failureInfo;
        this.scheduled = scheduled;
        this.self = requireNonNull(self, "self is null");
        this.query = requireNonNull(query, "query is null");
//        this.queryHash = computeQueryHash(query);
//        this.queryStats = requireNonNull(queryStats, "queryStats is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.warnings = requireNonNull(warnings, "warnings is null");
        this.preparedQuery = requireNonNull(preparedQuery, "preparedQuery is null");
    }

//    public BasicQueryInfo(
//            QueryId queryId,
//            Optional<ResourceGroupId> resourceGroupId,
//            MemoryPoolId memoryPool,
//            boolean scheduled,
//            URI self,
//            String query,
//            Optional<QueryType> queryType,
//            List<PrestoWarning> warnings,
//            Optional<String> preparedQuery)
//    {
//        this(
//                queryId,
//                resourceGroupId,
//                memoryPool,
//                scheduled,
//                self,
//                query,
//                queryType,
//                warnings,
//                preparedQuery);
//    }

//    public BasicQueryInfo(QueryInfo queryInfo)
//    {
//        this(queryInfo.getQueryId(),
//                queryInfo.getResourceGroupId(),
//                queryInfo.getMemoryPool(),
//                queryInfo.isScheduled(),
//                queryInfo.getSelf(),
//                queryInfo.getQuery(),
//                queryInfo.getErrorType(),
//                queryInfo.getErrorCode(),
//                queryInfo.getQueryType(),
//                queryInfo.getWarnings(),
//                queryInfo.getPreparedQuery());
//    }

//    public static BasicQueryInfo immediateFailureQueryInfo(Session session, String query, URI self, Optional<ResourceGroupId> resourceGroupId, ExecutionFailureInfo failure)
//    {
//        return new BasicQueryInfo(
//                session.getQueryId(),
//                session.toSessionRepresentation(),
//                resourceGroupId,
//                FAILED,
//                GENERAL_POOL,
//                false,
//                self,
//                query,
//                immediateFailureQueryStats(),
//                failure,
//                Optional.empty(),
//                ImmutableList.of(),
//                Optional.empty());
//    }

    @ThriftField(1)
    @JsonProperty
    public QueryId getQueryId()
    {
        return queryId;
    }

    @ThriftField(3)
    @JsonProperty
    public Optional<ResourceGroupId> getResourceGroupId()
    {
        return resourceGroupId;
    }


    @ThriftField(5)
    @JsonProperty
    public MemoryPoolId getMemoryPool()
    {
        return memoryPool;
    }

    @ThriftField(6)
    @JsonProperty
    public boolean isScheduled()
    {
        return scheduled;
    }

    @ThriftField(7)
    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @ThriftField(8)
    @JsonProperty
    public String getQuery()
    {
        return query;
    }

//    @ThriftField(9)
//    @JsonProperty
//    public String getQueryHash()
//    {
//        return queryHash;
//    }


    @ThriftField(11)
    @Nullable
    @JsonProperty
    public ErrorType getErrorType()
    {
        return errorType;
    }

    @ThriftField(12)
    @Nullable
    @JsonProperty
    public ErrorCode getErrorCode()
    {
        return errorCode;
    }


    @ThriftField(14)
    @JsonProperty
    public Optional<QueryType> getQueryType()
    {
        return queryType;
    }

    @ThriftField(15)
    @JsonProperty
    public List<PrestoWarning> getWarnings()
    {
        return warnings;
    }

    @ThriftField(16)
    @JsonProperty
    public Optional<String> getPreparedQuery()
    {
        return preparedQuery;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
//                .add("state", state)
                .toString();
    }
}
