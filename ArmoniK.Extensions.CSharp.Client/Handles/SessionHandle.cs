// This file is part of the ArmoniK project
// 
// Copyright (C) ANEO, 2021-2026. All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using ArmoniK.Extensions.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Client.Exceptions;
using ArmoniK.Extensions.CSharp.Client.Queryable;
using ArmoniK.Extensions.CSharp.Client.Services;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Common.Library;
using ArmoniK.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extensions.CSharp.Client.Handles;

/// <summary>
///   Handles session management operations for an ArmoniK client, providing methods to control the lifecycle of a
///   session.
/// </summary>
public class SessionHandle : IAsyncDisposable, IDisposable
{
  /// <summary>
  ///   The ArmoniK client used for performing blob operations.
  /// </summary>
  public readonly ArmoniKClient ArmoniKClient;

  /// <summary>
  ///   The session containing session ID
  /// </summary>
  public readonly SessionInfo SessionInfo;

  private readonly bool                 closeOnDispose_;
  private readonly object               locker_ = new();
  private          BackgroundSubmitter? backgroundSubmitter_;

  private int isDisposed_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="SessionHandle" /> class.
  /// </summary>
  /// <param name="session">The session information for managing session-related operations.</param>
  /// <param name="armoniKClient">The ArmoniK client used to perform operations on the session.</param>
  /// <param name="closeOnDispose">Whether the session should be closed once the SessionHandle instance is disposed.</param>
  internal SessionHandle(SessionInfo   session,
                         ArmoniKClient armoniKClient,
                         bool          closeOnDispose = false)
  {
    ArmoniKClient = armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient));
    SessionInfo   = session       ?? throw new ArgumentNullException(nameof(session));
    backgroundSubmitter_ = new BackgroundSubmitter(armoniKClient,
                                                   session,
                                                   CancellationToken.None);
    closeOnDispose_ = closeOnDispose;
  }

  /// <summary>
  ///   Dispose the resources of the session
  /// </summary>
  public async ValueTask DisposeAsync()
  {
    if (!TestAndSetDisposed())
    {
      await CancelCallbacksAsync()
        .ConfigureAwait(false);
      if (closeOnDispose_)
      {
        try
        {
          await CloseSessionAsync(CancellationToken.None)
            .ConfigureAwait(false);
        }
        catch
        {
          // Whenever the session is already closed
        }
      }
    }
  }

  /// <summary>
  ///   Dispose the resources of the session
  /// </summary>
  public void Dispose()
    => DisposeAsync()
      .WaitSync();

  private BackgroundSubmitter? TestAndSetBackgroundSubmitter()
  {
    lock (locker_)
    {
      var ret = backgroundSubmitter_;
      backgroundSubmitter_ = null;
      return ret;
    }
  }

  private BackgroundSubmitter CreateBackgroundSubmitterIfNeeded(CancellationToken cancellationToken)
  {
    lock (locker_)
    {
      return backgroundSubmitter_ ??= new BackgroundSubmitter(ArmoniKClient,
                                                              SessionInfo,
                                                              cancellationToken);
    }
  }

  private bool TestAndSetDisposed()
    => Interlocked.Exchange(ref isDisposed_,
                            1) != 0;

  /// <summary>
  ///   Implicit conversion operator from SessionHandle to SessionInfo.
  ///   Allows SessionHandle to be used wherever SessionInfo is expected.
  /// </summary>
  /// <param name="sessionHandle">The SessionHandle to convert.</param>
  /// <returns>The SessionInfo contained within the SessionHandle.</returns>
  /// <exception cref="ArgumentNullException">Thrown when sessionHandle is null.</exception>
  public static implicit operator SessionInfo(SessionHandle sessionHandle)
    => sessionHandle?.SessionInfo ?? throw new ArgumentNullException(nameof(sessionHandle));

  /// <summary>
  ///   Cancels the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task CancelSessionAsync(CancellationToken cancellationToken)
    => await ArmoniKClient.SessionService.CancelSessionAsync(SessionInfo,
                                                             cancellationToken)
                          .ConfigureAwait(false);

  /// <summary>
  ///   Closes the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task CloseSessionAsync(CancellationToken cancellationToken)
    => await ArmoniKClient.SessionService.CloseSessionAsync(SessionInfo,
                                                            cancellationToken)
                          .ConfigureAwait(false);

  /// <summary>
  ///   Pauses the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task PauseSessionAsync(CancellationToken cancellationToken)
    => await ArmoniKClient.SessionService.PauseSessionAsync(SessionInfo,
                                                            cancellationToken)
                          .ConfigureAwait(false);

  /// <summary>
  ///   Stops submissions to the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task StopSubmissionAsync(CancellationToken cancellationToken)
    => await ArmoniKClient.SessionService.StopSubmissionAsync(SessionInfo,
                                                              cancellationToken)
                          .ConfigureAwait(false);

  /// <summary>
  ///   Resumes the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task ResumeSessionAsync(CancellationToken cancellationToken)
    => await ArmoniKClient.SessionService.ResumeSessionAsync(SessionInfo,
                                                             cancellationToken)
                          .ConfigureAwait(false);

  /// <summary>
  ///   Purges the session asynchronously, removing all data associated with it.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task PurgeSessionAsync(CancellationToken cancellationToken)
    => await ArmoniKClient.SessionService.PurgeSessionAsync(SessionInfo,
                                                            cancellationToken)
                          .ConfigureAwait(false);

  /// <summary>
  ///   Deletes the session asynchronously.
  /// </summary>
  /// <param name="cancellationToken">A token that allows processing to be cancelled.</param>
  public async Task DeleteSessionAsync(CancellationToken cancellationToken)
    => await ArmoniKClient.SessionService.DeleteSessionAsync(SessionInfo,
                                                             cancellationToken)
                          .ConfigureAwait(false);

  /// <summary>
  ///   Asynchronously sends a dynamic library blob to a blob service
  /// </summary>
  /// <param name="dynamicLibrary">The dynamic library related to the blob being sent.</param>
  /// <param name="zipPath">File path to the zipped library.</param>
  /// <param name="manualDeletion">Whether the blob should be deleted manually.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task SendDllBlobAsync(DynamicLibrary    dynamicLibrary,
                                     string            zipPath,
                                     bool              manualDeletion,
                                     CancellationToken cancellationToken)
    => await ArmoniKClient.BlobService.SendDllBlobAsync(SessionInfo,
                                                        dynamicLibrary,
                                                        zipPath,
                                                        manualDeletion,
                                                        cancellationToken)
                          .ConfigureAwait(false);

  /// <summary>
  ///   Asynchronously waits for pending task submissions and callbacks to be completed.
  /// </summary>
  /// <returns> A task representing the asynchronous operation. </returns>
  public async Task WaitCallbacksAsync()
  {
    var backgroundSubmitter = TestAndSetBackgroundSubmitter();
    if (backgroundSubmitter != null)
    {
      await backgroundSubmitter.WaitAsync()
                               .ConfigureAwait(false);
    }
  }

  /// <summary>
  ///   Cancels all pending task submissions and callbacks.
  /// </summary>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task CancelCallbacksAsync()
  {
    var backgroundSubmitter = TestAndSetBackgroundSubmitter();
    if (backgroundSubmitter != null)
    {
      await backgroundSubmitter.DisposeAsync()
                               .ConfigureAwait(false);
    }
  }

  /// <summary>
  ///   Submit a task.
  /// </summary>
  /// <param name="task">The task to submit</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>The TaskHandle instance.</returns>
  /// <exception cref="ArgumentException">When the task parameter provided is null</exception>
  public TaskHandle Submit(TaskDefinition    task,
                           CancellationToken cancellationToken = default)
  {
    _ = task ?? throw new ArgumentNullException(nameof(task));

    var backgroundSubmitter = CreateBackgroundSubmitterIfNeeded(cancellationToken);
    var taskHandle          = TaskHandle.FromTaskCompletionSourceOfTaskInfos(ArmoniKClient);
    backgroundSubmitter.Add(task,
                            taskHandle);
    return taskHandle;
  }

  /// <summary>
  ///   Submit a collection of tasks.
  /// </summary>
  /// <param name="tasks">The tasks to submit</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>The collection of TaskHandle instances.</returns>
  /// <exception cref="ArgumentException">When the tasks parameter provided is null</exception>
  public ICollection<TaskHandle> Submit(ICollection<TaskDefinition> tasks,
                                        CancellationToken           cancellationToken = default)
  {
    _ = tasks ?? throw new ArgumentNullException(nameof(tasks));

    var backgroundSubmitter = CreateBackgroundSubmitterIfNeeded(cancellationToken);
    var taskHandles         = new TaskHandle[tasks.Count];
    for (var i = 0; i < tasks.Count; i++)
    {
      taskHandles[i] = TaskHandle.FromTaskCompletionSourceOfTaskInfos(ArmoniKClient);
      backgroundSubmitter.Add(tasks.ElementAt(i),
                              taskHandles[i]);
    }

    return taskHandles;
  }

  private class BackgroundSubmitter : IAsyncDisposable
  {
    private readonly ArmoniKClient armoniKClient_;
    private readonly ILogger       logger_;
    private readonly SessionInfo   sessionInfo_;

    /// <summary>
    ///   Cancels any new submission.
    /// </summary>
    private readonly CancellationTokenSource submissionCts_;

    private readonly Task submissionTask_;

    private readonly Channel<(TaskDefinition, TaskHandle)> taskSubmissionChannel_;
    private          bool                                  disposed_;

    public BackgroundSubmitter(ArmoniKClient     armoniKClient,
                               SessionInfo       sessionInfo,
                               CancellationToken cancellationToken)
    {
      armoniKClient_ = armoniKClient;
      sessionInfo_   = sessionInfo;
      submissionCts_ = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
      taskSubmissionChannel_ = Channel.CreateUnbounded<(TaskDefinition, TaskHandle)>(new UnboundedChannelOptions
                                                                                     {
                                                                                       SingleReader = true,
                                                                                       SingleWriter = true,
                                                                                     });
      submissionTask_ = Task.Run(RunSubmitterAsync);
      logger_         = armoniKClient.LoggerFactory.CreateLogger<BackgroundSubmitter>();
    }

    public async ValueTask DisposeAsync()
    {
      if (!disposed_)
      {
        disposed_ = true;

        // Send an abort signal
        submissionCts_.Cancel();

        // Wait for the worker task to complete
        await submissionTask_.ConfigureAwait(false);

        submissionCts_.Dispose();
      }
    }

    public void Add(TaskDefinition taskDefinition,
                    TaskHandle     taskHandle)
      => taskSubmissionChannel_.Writer.WriteAsync((taskDefinition, taskHandle));

    /// <summary>
    ///   Wait until all registered tasks are submitted.
    /// </summary>
    /// <returns>
    ///   A task representing the asynchronous operation.
    /// </returns>
    public async Task WaitAsync()
    {
      if (taskSubmissionChannel_.Writer.TryComplete())
      {
        await submissionTask_.ConfigureAwait(false);
        await DisposeAsync()
          .ConfigureAwait(false);
      }
    }

    private async Task RunSubmitterAsync()
    {
      var callbackRunner = new CallbackRunner(armoniKClient_,
                                              submissionCts_.Token);
      try
      {
        await foreach (var chunk in taskSubmissionChannel_.Reader.ToAsyncEnumerable(submissionCts_.Token)
                                                          .ToChunksAsync(1000,
                                                                         TimeSpan.FromSeconds(5),
                                                                         submissionCts_.Token)
                                                          .ConfigureAwait(false))
        {
          TaskInfos[]? taskInfos = null;
          try
          {
            taskInfos = await armoniKClient_.TasksService.SubmitTasksAsync(sessionInfo_,
                                                                           chunk.ViewSelect(tuple => tuple.Item1),
                                                                           submissionCts_.Token)
                                            .ToArrayAsync(submissionCts_.Token)
                                            .ConfigureAwait(false);
          }
          catch (Exception ex)
          {
            logger_.LogError(ex,
                             "Failure to submit Tasks");
            foreach (var task in chunk)
            {
              var taskHandle = task.Item2;
              taskHandle.TaskInfosSource!.SetException(ex);
            }

            continue;
          }

          var enumTask = chunk.AsEnumerable()
                              .GetEnumerator();
          foreach (var taskInfo in taskInfos)
          {
            enumTask.MoveNext();
            var taskDefinition = enumTask.Current.Item1;
            var taskHandle     = enumTask.Current.Item2;
            taskHandle.TaskInfosSource!.SetResult(taskInfo);

            var blobsWithCallbacks = taskDefinition.Outputs.Values.Where(b => b.CallBack != null);
            if (blobsWithCallbacks.Any())
            {
              foreach (var blob in blobsWithCallbacks)
              {
                callbackRunner.Add(blob);
              }
            }
          }
        }
      }
      catch (OperationCanceledException)
      {
      }
      finally
      {
        // If submission was canceled, callbacks were canceled too,
        // then there is no need to wait the callback runner to complete.
        if (!submissionCts_.Token.IsCancellationRequested)
        {
          await callbackRunner.WaitAsync()
                              .ConfigureAwait(false);
        }

        await callbackRunner.DisposeAsync()
                            .ConfigureAwait(false);
      }
    }
  }

  private class CallbackRunner : IAsyncDisposable
  {
    /// <summary>
    ///   Cancels any callback being executed. When cancelled, cancels loopsCts_ as well.
    /// </summary>
    private readonly CancellationTokenSource callbacksCts_;

    /// <summary>
    ///   Dictionary taking a blob is as key, a callback as value.
    /// </summary>
    private readonly ConcurrentDictionary<string, ICallback> callbacks_ = new();

    private readonly ArmoniKClient client_;

    private readonly object locker_ = new();

    /// <summary>
    ///   Exits the execution loop when cancellation is requested.
    /// </summary>
    private readonly CancellationTokenSource loopCts_;

    private readonly Task                        workerTask_;
    private          TaskCompletionSource<bool>? taskCompletionSource_;

    public CallbackRunner(ArmoniKClient     client,
                          CancellationToken cancellationToken)
    {
      client_       = client;
      callbacksCts_ = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
      loopCts_      = CancellationTokenSource.CreateLinkedTokenSource(callbacksCts_.Token);
      workerTask_   = Task.Run(RunAsync);
    }

    /// <summary>
    ///   Exits the execution loop and disposes resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
      // Send an abort signal
      loopCts_.Cancel();
      // Wait for the worker task to complete
      await workerTask_.ConfigureAwait(false);
      // Dispose disposable members
      loopCts_.Dispose();
      callbacksCts_.Dispose();
    }

    /// <summary>
    ///   Cancels the execution of pending callbacks and ongoing callback executions.
    /// </summary>
    public void AbortCallbacks()
      => callbacksCts_.Cancel();

    private TaskCompletionSource<bool>? CreateTaskCompletionSource()
    {
      lock (locker_)
      {
        return taskCompletionSource_ ??= new TaskCompletionSource<bool>();
      }
    }

    private void ResetTaskCompletionSource()
    {
      lock (locker_)
      {
        taskCompletionSource_ = null;
      }
    }

    private TaskCompletionSource<bool>? GetTaskCompletionSource()
    {
      lock (locker_)
      {
        return taskCompletionSource_;
      }
    }

    /// <summary>
    ///   Wait until all registered callbacks are executed.
    /// </summary>
    /// <returns>
    ///   A task representing the asynchronous operation. The task result is true when all callbacks were invoked, false
    ///   otherwise.
    /// </returns>
    public async Task<bool> WaitAsync()
    {
      var taskCompletionSource = CreateTaskCompletionSource();
      return await taskCompletionSource!.Task.ConfigureAwait(false);
    }

    /// <summary>
    ///   Register a new blob having a callback.
    /// </summary>
    /// <param name="blob">The blob definition carrying a callback</param>
    public void Add(BlobDefinition blob)
      => callbacks_.GetOrAdd(blob.BlobHandle!.BlobInfo.BlobId,
                             blob.CallBack!);

    private async Task ExecuteCallback((ICallback callback, BlobState blobState) tuple)
    {
      var blobHandle = new BlobHandle(tuple.blobState,
                                      client_);
      switch (tuple.blobState.Status)
      {
        case BlobStatus.Completed:
          try
          {
            var result = await blobHandle.DownloadBlobDataAsync(callbacksCts_.Token)
                                         .ConfigureAwait(false);
            await tuple.callback.OnSuccessAsync(blobHandle,
                                                result,
                                                callbacksCts_.Token)
                       .ConfigureAwait(false);
          }
          catch (Exception ex)
          {
            await tuple.callback.OnErrorAsync(blobHandle,
                                              ex,
                                              callbacksCts_.Token)
                       .ConfigureAwait(false);
          }

          break;
        case BlobStatus.Aborted:
          await tuple.callback.OnErrorAsync(blobHandle,
                                            new ArmoniKSdkException("blob aborted, call of OnSuccessAsync() was canceled."),
                                            callbacksCts_.Token)
                     .ConfigureAwait(false);
          break;
        case BlobStatus.Deleted:
          await tuple.callback.OnErrorAsync(blobHandle,
                                            new ArmoniKSdkException("blob deleted, call of OnSuccessAsync() was canceled."),
                                            callbacksCts_.Token)
                     .ConfigureAwait(false);
          break;
        default:
          await tuple.callback.OnErrorAsync(blobHandle,
                                            new ArmoniKSdkException("blob is in an inconsistent state, call of OnSuccessAsync() was canceled."),
                                            callbacksCts_.Token)
                     .ConfigureAwait(false);
          break;
      }
    }

    private async IAsyncEnumerable<BlobState> GetBlobStates(ICollection<string>                        blobIds,
                                                            [EnumeratorCancellation] CancellationToken cancellationToken)
    {
      var pageSize = 1000;
      foreach (var chunk in blobIds.ToChunks(pageSize))
      {
        var query = client_.BlobService.AsQueryable()
                           .Where(blobState => chunk.Contains(blobState.BlobId) && blobState.Status != BlobStatus.Created)
                           .WithPageSize(pageSize);
        await foreach (var blobState in query.ToAsyncEnumerable()
                                             .WithCancellation(cancellationToken)
                                             .ConfigureAwait(false))
        {
          yield return blobState;
        }
      }
    }

    /// <summary>
    ///   Execution loop
    /// </summary>
    private async Task RunAsync()
    {
      TaskCompletionSource<bool>? taskCompletionSource = null;

      var channel = Channel.CreateUnbounded<(ICallback callback, BlobState blobState)>(new UnboundedChannelOptions
                                                                                       {
                                                                                         SingleReader = true,
                                                                                         SingleWriter = true,
                                                                                       });
      var channelReceiverTask = channel.Reader.ToAsyncEnumerable(callbacksCts_.Token)
                                       .ParallelForEach(new ParallelTaskOptions
                                                        {
                                                          ParallelismLimit  = client_.Properties.ParallelismLimit,
                                                          Unordered         = true,
                                                          CancellationToken = callbacksCts_.Token,
                                                        },
                                                        ExecuteCallback);

      try
      {
        while (!loopCts_.Token.IsCancellationRequested)
        {
          // Loop on completed blobs
          await foreach (var blobState in GetBlobStates(callbacks_.Keys,
                                                        loopCts_.Token)
                           .ConfigureAwait(false))
          {
            if (callbacks_.TryRemove(blobState.BlobId,
                                     out var func))
            {
              await channel.Writer.WriteAsync((func, blobState),
                                              loopCts_.Token)
                           .ConfigureAwait(false);
            }
            else
            {
              throw new ArmoniKSdkException($"Unexpected error: could not retrieve the callback for blob {blobState.BlobId}");
            }
          }

          taskCompletionSource = GetTaskCompletionSource();
          if (taskCompletionSource != null && callbacks_.IsEmpty)
          {
            taskCompletionSource.SetResult(true);
            ResetTaskCompletionSource();
          }

          // Wait for 5 second and then retry
          await Task.Delay(5000,
                           loopCts_.Token)
                    .ConfigureAwait(false);
        }

        taskCompletionSource = GetTaskCompletionSource();
        taskCompletionSource?.SetResult(false);
      }
      catch (OperationCanceledException)
      {
        taskCompletionSource = GetTaskCompletionSource();
        taskCompletionSource?.SetResult(false);
      }
      finally
      {
        channel.Writer.TryComplete();
      }

      try
      {
        await channelReceiverTask.ConfigureAwait(false);
      }
      catch (OperationCanceledException) when (loopCts_.Token.IsCancellationRequested)
      {
        // Ignore cancellation exceptions during shutdown
      }
    }
  }
}
