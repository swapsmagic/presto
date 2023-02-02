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
package com.facebook.presto.dispatcher;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.event.QueryMonitor;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.execution.ClusterSizeMonitor;
import com.facebook.presto.execution.ExecutionFailureInfo;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStateMachine;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.prerequisites.QueryPrerequisites;
import com.facebook.presto.spi.prerequisites.QueryPrerequisitesContext;
import com.facebook.presto.spi.resourceGroups.ResourceGroupQueryLimits;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.facebook.airlift.concurrent.MoreFutures.addExceptionCallback;
import static com.facebook.airlift.concurrent.MoreFutures.addSuccessCallback;
import static com.facebook.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.USER_CANCELED;
import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class RMDispatchQuery
        implements DispatchQuery
{
    private static final Logger log = Logger.get(RMDispatchQuery.class);
    private final QueryStateMachine stateMachine;
    private final QueryMonitor queryMonitor;
//    private final ListenableFuture<QueryExecution> queryExecutionFuture;

    private final ClusterSizeMonitor clusterSizeMonitor;

    private final Executor queryExecutor;

    private final Consumer<DispatchQuery> queryQueuer;
    private final Consumer<QueryExecution> querySubmitter;
    private final SettableFuture<?> submitted = SettableFuture.create();
    private final AtomicReference<Optional<ResourceGroupQueryLimits>> resourceGroupQueryLimits = new AtomicReference<>(Optional.empty());

    private final boolean retry;

    private final QueryPrerequisites queryPrerequisites;
    private final WarningCollector warningCollector;
    private final InternalNodeManager internalNodeManager;

    /**
     * Local dispatch query encapsulates QueryExecution and submit to the ResourceGroupManager waiting for resource to get executed.
     *
     * @param stateMachine the state machine to keep track of the state of the query
     * @param queryMonitor the query monitor records information to the {@link EventListenerManager}
//     * @param queryExecutionFuture the query execution future
     * @param clusterSizeMonitor the cluster size monitor provides a method to obtain a listener object when the minimum number of workers for the cluster has been met
     * @param queryExecutor the query executor is used to start a future for query to get executed. This will trigger the query execution phase by involving {@code querySubmitter}
     * @param queryQueuer the query queuer is used to register the query that is being queued while waiting for query prerequisites being returned
     * @param querySubmitter the query submitter takes in query execution object. This will trigger query to start executed with {@link com.facebook.presto.execution.SqlQueryManager}
     * @param retry if this is a retry query
     * @param queryPrerequisites the query prerequisites are conditions when the query is ready to be queued for execution
     */
    public RMDispatchQuery(
            QueryStateMachine stateMachine,
            QueryMonitor queryMonitor,
//            ListenableFuture<QueryExecution> queryExecutionFuture,
            ClusterSizeMonitor clusterSizeMonitor,
            Executor queryExecutor,
            Consumer<DispatchQuery> queryQueuer,
            Consumer<QueryExecution> querySubmitter,
            boolean retry,
            QueryPrerequisites queryPrerequisites,
            InternalNodeManager internalNodeManager)
    {
        this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
//        this.queryExecutionFuture = requireNonNull(queryExecutionFuture, "queryExecutionFuture is null");
        this.clusterSizeMonitor = requireNonNull(clusterSizeMonitor, "clusterSizeMonitor is null");
        this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
        this.queryQueuer = requireNonNull(queryQueuer, "queryQueuer is null");
        this.querySubmitter = requireNonNull(querySubmitter, "querySubmitter is null");
        this.retry = retry;
        this.queryPrerequisites = requireNonNull(queryPrerequisites, "queryPrerequisites is null");
        this.warningCollector = requireNonNull(stateMachine.getWarningCollector(), "warningCollector is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
//        addExceptionCallback(queryExecutionFuture, throwable -> {
//            if (stateMachine.transitionToFailed(throwable)) {
//                queryMonitor.queryImmediateFailureEvent(stateMachine.getBasicQueryInfo(Optional.empty()), toFailure(throwable));
//            }
//        });
        stateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                submitted.set(null);
            }
        });
    }

    @Override
    public void startWaitingForPrerequisites()
    {
        // It's possible that queryExecution fails before we start for prerequisites, in that case, don't even
        // start waiting for prerequisites
        if (isDone()) {
            return;
        }

        try {
            Session session = stateMachine.getSession();
            CompletableFuture<?> prerequisitesFuture = queryPrerequisites.waitForPrerequisites(
                    stateMachine.getQueryId(),
                    new QueryPrerequisitesContext(
                            session.getCatalog(),
                            session.getSchema(),
                            stateMachine.getBasicQueryInfo(Optional.empty()).getQuery(),
                            session.getSystemProperties(),
                            session.getConnectorProperties()),
                    warningCollector);

            addStateChangeListener(state -> {
                if (state.isDone()) {
                    queryPrerequisites.queryFinished(stateMachine.getQueryId());
                    if (!prerequisitesFuture.isDone()) {
                        prerequisitesFuture.cancel(true);
                    }
                }
            });

            prerequisitesFuture.whenCompleteAsync((result, throwable) -> {
                if (throwable != null) {
                    fail(throwable);
                    return;
                }

                queueQuery();
            }, queryExecutor);
        }
        catch (Throwable t) {
            fail(t);
            throw t;
        }
    }

    private void queueQuery()
    {
        if (stateMachine.transitionToQueued()) {
            try {
                queryQueuer.accept(this);
            }
            catch (Throwable t) {
                fail(t);
            }
        }
    }

    @Override
    public void startWaitingForResources()
    {
        if (stateMachine.transitionToDispatched()) {
            submitted.set(null);
        }
    }

    private void startExecution(QueryExecution queryExecution, boolean isDispatching)
    {
        queryExecutor.execute(() -> {
            if (isDispatching) {
                try {
                    resourceGroupQueryLimits.get().ifPresent(queryExecution::setResourceGroupQueryLimits);
                    querySubmitter.accept(queryExecution);
                }
                catch (Throwable t) {
                    // this should never happen but be safe
                    fail(t);
                    log.error(t, "query submitter threw exception");
                    throw t;
                }
                finally {
                    submitted.set(null);
                }
            }
        });
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public ListenableFuture<?> getDispatchedFuture()
    {
        return nonCancellationPropagating(submitted);
    }

    @Override
    public DispatchInfo getDispatchInfo()
    {
        // observe submitted before getting the state, to ensure a failed query stat is visible
        boolean dispatched = submitted.isDone();
        BasicQueryInfo queryInfo = stateMachine.getBasicQueryInfo(Optional.empty());

        if (queryInfo.getState() == FAILED) {
            ExecutionFailureInfo failureInfo = stateMachine.getFailureInfo()
                    .orElseGet(() -> toFailure(new PrestoException(GENERIC_INTERNAL_ERROR, "Query failed for an unknown reason")));
            return DispatchInfo.failed(failureInfo, queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getWaitingForPrerequisitesTime(), queryInfo.getQueryStats().getQueuedTime());
        }
        if (dispatched) {
            return DispatchInfo.dispatched(new RemoteCoordinatorLocation(internalNodeManager), queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getWaitingForPrerequisitesTime(), queryInfo.getQueryStats().getQueuedTime());
        }
        if (queryInfo.getState() == QUEUED) {
            return DispatchInfo.queued(queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getWaitingForPrerequisitesTime(), queryInfo.getQueryStats().getQueuedTime());
        }
        return DispatchInfo.waitingForPrerequisites(queryInfo.getQueryStats().getElapsedTime(), queryInfo.getQueryStats().getWaitingForPrerequisitesTime());
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public boolean isDone()
    {
        return stateMachine.getQueryState() == QueryState.DISPATCHED;
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        return new Duration(0, MILLISECONDS);
//        return tryGetQueryExecution()
//                .map(QueryExecution::getTotalCpuTime)
//                .orElse(new Duration(0, MILLISECONDS));
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        return new DataSize(0, BYTE);
//        return tryGetQueryExecution()
//                .map(QueryExecution::getTotalMemoryReservation)
//                .orElse(new DataSize(0, BYTE));
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        return new DataSize(0, BYTE);
//        return tryGetQueryExecution()
//                .map(QueryExecution::getUserMemoryReservation)
//                .orElse(new DataSize(0, BYTE));
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getBasicQueryInfo(Optional.empty());
//        return tryGetQueryExecution()
//                .map(QueryExecution::getBasicQueryInfo)
//                .orElse(stateMachine.getBasicQueryInfo(Optional.empty()));
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public void fail(Throwable throwable)
    {
        if (stateMachine.transitionToFailed(throwable)) {
            queryMonitor.queryImmediateFailureEvent(stateMachine.getBasicQueryInfo(Optional.empty()), toFailure(throwable));
        }
    }

    @Override
    public void cancel()
    {
        if (stateMachine.transitionToCanceled()) {
            BasicQueryInfo queryInfo = stateMachine.getBasicQueryInfo(Optional.empty());
            ExecutionFailureInfo failureInfo = queryInfo.getFailureInfo();
            failureInfo = failureInfo != null ? failureInfo : toFailure(new PrestoException(USER_CANCELED, "Query was canceled"));
            queryMonitor.queryImmediateFailureEvent(queryInfo, failureInfo);
        }
    }

    @Override
    public void pruneInfo()
    {
        stateMachine.pruneQueryInfo();
    }

    @Override
    public Optional<ErrorCode> getErrorCode()
    {
        return stateMachine.getFailureInfo().map(ExecutionFailureInfo::getErrorCode);
    }

    @Override
    public boolean isRetry()
    {
        return retry;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateMachine.addStateChangeListener(stateChangeListener);
    }

    @Override
    public Optional<ResourceGroupQueryLimits> getResourceGroupQueryLimits()
    {
        return resourceGroupQueryLimits.get();
    }

    @Override
    public void setResourceGroupQueryLimits(ResourceGroupQueryLimits resourceGroupQueryLimits)
    {
        if (!this.resourceGroupQueryLimits.compareAndSet(Optional.empty(), Optional.of(requireNonNull(resourceGroupQueryLimits, "resourceGroupQueryLimits is null")))) {
            throw new IllegalStateException("Cannot set resourceGroupQueryLimits more than once");
        }
    }

//    private Optional<QueryExecution> tryGetQueryExecution()
//    {
//        try {
//            return tryGetFutureValue(queryExecutionFuture);
//        }
//        catch (Throwable ignored) {
//            return Optional.empty();
//        }
//    }
}
