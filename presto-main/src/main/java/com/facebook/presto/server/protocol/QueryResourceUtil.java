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
package com.facebook.presto.server.protocol;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.client.QueryResults;
import com.facebook.presto.client.StageStats;
import com.facebook.presto.client.StatementStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.execution.StageExecutionInfo;
import com.facebook.presto.execution.StageExecutionStats;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.security.SelectedRole;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import io.airlift.units.Duration;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREFIX_URL;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_REMOVED_SESSION_FUNCTION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_ROLE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.facebook.presto.execution.QueryState.DISPATCHED;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;

public final class QueryResourceUtil
{
    private static final Logger log = Logger.get(QueryResourceUtil.class);
    private static final JsonCodec<SqlFunctionId> SQL_FUNCTION_ID_JSON_CODEC = jsonCodec(SqlFunctionId.class);
    private static final JsonCodec<SqlInvokedFunction> SQL_INVOKED_FUNCTION_JSON_CODEC = jsonCodec(SqlInvokedFunction.class);

    private static final Duration NO_DURATION = new Duration(0, MILLISECONDS);

    private QueryResourceUtil() {}

    public static Response toResponse(Query query, QueryResults queryResults, boolean compressionEnabled)
    {
        Response.ResponseBuilder response = Response.ok(queryResults);

        // add set catalog and schema
        query.getSetCatalog().ifPresent(catalog -> response.header(PRESTO_SET_CATALOG, catalog));
        query.getSetSchema().ifPresent(schema -> response.header(PRESTO_SET_SCHEMA, schema));

        // add set session properties
        query.getSetSessionProperties()
                .forEach((key, value) -> response.header(PRESTO_SET_SESSION, key + '=' + urlEncode(value)));

        // add clear session properties
        query.getResetSessionProperties()
                .forEach(name -> response.header(PRESTO_CLEAR_SESSION, name));

        // add set roles
        query.getSetRoles()
                .forEach((key, value) -> response.header(PRESTO_SET_ROLE, key + '=' + urlEncode(value.toString())));

        // add added prepare statements
        for (Map.Entry<String, String> entry : query.getAddedPreparedStatements().entrySet()) {
            String encodedKey = urlEncode(entry.getKey());
            String encodedValue = urlEncode(entry.getValue());
            response.header(PRESTO_ADDED_PREPARE, encodedKey + '=' + encodedValue);
        }

        // add deallocated prepare statements
        for (String name : query.getDeallocatedPreparedStatements()) {
            response.header(PRESTO_DEALLOCATED_PREPARE, urlEncode(name));
        }

        // add new transaction ID
        query.getStartedTransactionId()
                .ifPresent(transactionId -> response.header(PRESTO_STARTED_TRANSACTION_ID, transactionId));

        // add clear transaction ID directive
        if (query.isClearTransactionId()) {
            response.header(PRESTO_CLEAR_TRANSACTION_ID, true);
        }

        if (!compressionEnabled) {
            response.encoding("identity");
        }

        // add added session functions
        for (Map.Entry<SqlFunctionId, SqlInvokedFunction> entry : query.getAddedSessionFunctions().entrySet()) {
            response.header(PRESTO_ADDED_SESSION_FUNCTION, format(
                    "%s=%s",
                    urlEncode(SQL_FUNCTION_ID_JSON_CODEC.toJson(entry.getKey())),
                    urlEncode(SQL_INVOKED_FUNCTION_JSON_CODEC.toJson(entry.getValue()))));
        }

        // add removed session functions
        for (SqlFunctionId signature : query.getRemovedSessionFunctions()) {
            response.header(PRESTO_REMOVED_SESSION_FUNCTION, urlEncode(SQL_FUNCTION_ID_JSON_CODEC.toJson(signature)));
        }

        return response.build();
    }

    public static Response toResponse(Query query, QueryResults queryResults, String xPrestoPrefixUri, boolean compressionEnabled)
    {
        QueryResults resultsClone = new QueryResults(
                queryResults.getId(),
                prependUri(queryResults.getInfoUri(), xPrestoPrefixUri),
                prependUri(queryResults.getPartialCancelUri(), xPrestoPrefixUri),
                prependUri(queryResults.getNextUri(), xPrestoPrefixUri),
                "GET",
                queryResults.getColumns(),
                queryResults.getData(),
                queryResults.getStats(),
                queryResults.getError(),
                queryResults.getWarnings(),
                queryResults.getUpdateType(),
                queryResults.getUpdateCount());

        return toResponse(query, resultsClone, compressionEnabled);
    }

    public static Response toResponse(QueryId queryId, InternalNode coordinator, UriInfo uriInfo, String xPrestoPrefixUri,
            boolean compressionEnabled, Optional<String> catalog, Optional<String> schema, Map<String, String> setSessionProperties,
            Set<String> resetSessionProperties, Map<String, SelectedRole> setRoles,
            Map<String, String> addedPreparedStatements, Set<String> deAllocatedPreparedStatements)
    {
        URI infoUri = uriInfo.getRequestUriBuilder()
                .scheme(uriInfo.getBaseUri().getScheme())
                .replacePath("ui/query.html")
                .host(coordinator.getHost())
                .port(coordinator.getHostAndPort().getPort())
                .replaceQuery(queryId.toString())
                .build();

        URI nextUri = uriInfo.getBaseUriBuilder()
                .scheme(uriInfo.getBaseUri().getScheme())
                .replacePath("/v1/statement/queued1")
                .path(queryId.toString())
                .host(coordinator.getHost())
                .port(coordinator.getHostAndPort().getPort())
                .build();

        URI uri = uriBuilderFrom(uriInfo.getBaseUri()).host(coordinator.getHost()).port(coordinator.getHostAndPort().getPort()).build();

        QueryResults queryResults = new QueryResults(
                queryId.toString(),
                prependUri(infoUri, xPrestoPrefixUri),
                prependUri(coordinator.getInternalUri(), xPrestoPrefixUri),
                prependUri(nextUri, xPrestoPrefixUri),
                "POST",
                null,
                null,
                StatementStats.builder()
                        .setState(DISPATCHED.toString())
                        .setWaitingForPrerequisites(false)
                        .setElapsedTimeMillis(NO_DURATION.toMillis())
                        .setQueuedTimeMillis(NO_DURATION.toMillis())
                        .setWaitingForPrerequisitesTimeMillis(NO_DURATION.toMillis())
                        .build(),
                null,
                ImmutableList.of(),
                null,
                null);

        Response.ResponseBuilder response = Response.ok(queryResults);

        // add set catalog and schema
        catalog.ifPresent(c -> response.header(PRESTO_SET_CATALOG, c));
        schema.ifPresent(s -> response.header(PRESTO_SET_SCHEMA, s));

        // add set session properties
        setSessionProperties.forEach((key, value) -> response.header(PRESTO_SET_SESSION, key + '=' + urlEncode(value)));

        // add clear session properties
        resetSessionProperties.forEach(name -> response.header(PRESTO_CLEAR_SESSION, name));

        // add set roles
        setRoles.forEach((key, value) -> response.header(PRESTO_SET_ROLE, key + '=' + urlEncode(value.toString())));

        // add added prepare statements
        for (Map.Entry<String, String> entry : addedPreparedStatements.entrySet()) {
            String encodedKey = urlEncode(entry.getKey());
            String encodedValue = urlEncode(entry.getValue());
            response.header(PRESTO_ADDED_PREPARE, encodedKey + '=' + encodedValue);
        }

        // add deallocated prepare statements
        for (String name : deAllocatedPreparedStatements) {
            response.header(PRESTO_DEALLOCATED_PREPARE, urlEncode(name));
        }

        if (!compressionEnabled) {
            response.encoding("identity");
        }

        return response.build();
    }

    public static void abortIfPrefixUrlInvalid(String xPrestoPrefixUrl)
    {
        if (xPrestoPrefixUrl != null) {
            try {
                URL url = new URL(xPrestoPrefixUrl);
            }
            catch (java.net.MalformedURLException e) {
                throw new WebApplicationException(
                        Response.status(Response.Status.BAD_REQUEST)
                                .type(TEXT_PLAIN_TYPE)
                                .entity(PRESTO_PREFIX_URL + " is not a valid URL")
                                .build());
            }
        }
    }

    public static URI prependUri(URI backendUri, String xPrestoPrefixUrl)
    {
        if (!isNullOrEmpty(xPrestoPrefixUrl) && (backendUri != null)) {
            String encodedBackendUri = Base64.getUrlEncoder().encodeToString(backendUri.toASCIIString().getBytes());

            try {
                return new URI(xPrestoPrefixUrl + encodedBackendUri);
            }
            catch (URISyntaxException e) {
                log.error(e, "Unable to add Proxy Prefix to URL");
            }
        }

        return backendUri;
    }

    public static StatementStats toStatementStats(QueryInfo queryInfo)
    {
        QueryStats queryStats = queryInfo.getQueryStats();
        StageInfo outputStage = queryInfo.getOutputStage().orElse(null);

        Set<String> globalUniqueNodes = new HashSet<>();
        StageStats rootStageStats = toStageStats(outputStage, globalUniqueNodes);

        return StatementStats.builder()
                .setState(queryInfo.getState().toString())
                .setWaitingForPrerequisites(queryInfo.getState() == QueryState.WAITING_FOR_PREREQUISITES)
                .setQueued(queryInfo.getState() == QueryState.QUEUED)
                .setScheduled(queryInfo.isScheduled())
                .setNodes(globalUniqueNodes.size())
                .setTotalSplits(queryStats.getTotalDrivers())
                .setQueuedSplits(queryStats.getQueuedDrivers())
                .setRunningSplits(queryStats.getRunningDrivers() + queryStats.getBlockedDrivers())
                .setCompletedSplits(queryStats.getCompletedDrivers())
                .setCpuTimeMillis(queryStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(queryStats.getTotalScheduledTime().toMillis())
                .setWaitingForPrerequisitesTimeMillis(queryStats.getWaitingForPrerequisitesTime().toMillis())
                .setQueuedTimeMillis(queryStats.getQueuedTime().toMillis())
                .setElapsedTimeMillis(queryStats.getElapsedTime().toMillis())
                .setProcessedRows(queryStats.getRawInputPositions())
                .setProcessedBytes(queryStats.getRawInputDataSize().toBytes())
                .setPeakMemoryBytes(queryStats.getPeakUserMemoryReservation().toBytes())
                .setPeakTotalMemoryBytes(queryStats.getPeakTotalMemoryReservation().toBytes())
                .setPeakTaskTotalMemoryBytes(queryStats.getPeakTaskTotalMemory().toBytes())
                .setSpilledBytes(queryStats.getSpilledDataSize().toBytes())
                .setRootStage(rootStageStats)
                .setRuntimeStats(queryStats.getRuntimeStats())
                .build();
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    private static StageStats toStageStats(StageInfo stageInfo, Set<String> globalUniqueNodeIds)
    {
        if (stageInfo == null) {
            return null;
        }

        StageExecutionInfo currentStageExecutionInfo = stageInfo.getLatestAttemptExecutionInfo();
        StageExecutionStats stageExecutionStats = currentStageExecutionInfo.getStats();

        // Store current stage details into a builder
        StageStats.Builder builder = StageStats.builder()
                .setStageId(String.valueOf(stageInfo.getStageId().getId()))
                .setState(currentStageExecutionInfo.getState().toString())
                .setDone(currentStageExecutionInfo.getState().isDone())
                .setTotalSplits(stageExecutionStats.getTotalDrivers())
                .setQueuedSplits(stageExecutionStats.getQueuedDrivers())
                .setRunningSplits(stageExecutionStats.getRunningDrivers() + stageExecutionStats.getBlockedDrivers())
                .setCompletedSplits(stageExecutionStats.getCompletedDrivers())
                .setCpuTimeMillis(stageExecutionStats.getTotalCpuTime().toMillis())
                .setWallTimeMillis(stageExecutionStats.getTotalScheduledTime().toMillis())
                .setProcessedRows(stageExecutionStats.getRawInputPositions())
                .setProcessedBytes(stageExecutionStats.getRawInputDataSize().toBytes())
                .setNodes(countStageAndAddGlobalUniqueNodes(currentStageExecutionInfo.getTasks(), globalUniqueNodeIds));

        // Recurse into child stages to create their StageStats
        List<StageInfo> subStages = stageInfo.getSubStages();
        if (subStages.isEmpty()) {
            builder.setSubStages(ImmutableList.of());
        }
        else {
            ImmutableList.Builder<StageStats> subStagesBuilder = ImmutableList.builderWithExpectedSize(subStages.size());
            for (StageInfo subStage : subStages) {
                subStagesBuilder.add(toStageStats(subStage, globalUniqueNodeIds));
            }
            builder.setSubStages(subStagesBuilder.build());
        }

        return builder.build();
    }

    private static int countStageAndAddGlobalUniqueNodes(List<TaskInfo> tasks, Set<String> globalUniqueNodes)
    {
        Set<String> stageUniqueNodes = Sets.newHashSetWithExpectedSize(tasks.size());
        for (TaskInfo task : tasks) {
            String nodeId = task.getNodeId();
            stageUniqueNodes.add(nodeId);
            globalUniqueNodes.add(nodeId);
        }
        return stageUniqueNodes.size();
    }
}
