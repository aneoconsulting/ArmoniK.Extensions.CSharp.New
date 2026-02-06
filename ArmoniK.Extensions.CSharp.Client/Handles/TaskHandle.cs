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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;

namespace ArmoniK.Extensions.CSharp.Client.Handles;

/// <summary>
///   Handles operations related to tasks using the ArmoniK client.
/// </summary>
public class TaskHandle
{
  /// <summary>
  ///   Gets the ArmoniK client used to interact with task services.
  /// </summary>
  public readonly ArmoniKClient ArmoniKClient;

  /// <summary>
  ///   Promise of the TaskInfos when the task as been submitted.
  ///   It needs to be volatile as we need it to play the role of a barrier in GetTaskInfosAsync().
  /// </summary>
  private volatile TaskCompletionSource<TaskInfos>? taskInfosSource_;

  /// <summary>
  ///   Gets the task information for which this handle will perform operations.
  /// </summary>
  private TaskInfos? taskInfos_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="TaskHandle" /> class with a specified ArmoniK client and task
  ///   information.
  /// </summary>
  /// <param name="armoniKClient">The ArmoniK client to be used for task service operations.</param>
  /// <param name="taskInfo">The task information related to the tasks that will be handled.</param>
  /// <param name="source">The task information related to the tasks that will be handled.</param>
  /// <exception cref="ArgumentNullException">Thrown when armoniKClient or taskInfo is null.</exception>
  private TaskHandle(ArmoniKClient                    armoniKClient,
                     TaskInfos?                       taskInfo,
                     TaskCompletionSource<TaskInfos>? source)
  {
    ArmoniKClient    = armoniKClient;
    taskInfos_       = taskInfo;
    taskInfosSource_ = source;
  }

  /// <summary>
  ///   The TaskCompletionSource valued by the task submission.
  /// </summary>
  internal TaskCompletionSource<TaskInfos>? TaskInfosSource
    => taskInfosSource_;

  /// <summary>
  ///   Creates a TaskHandle from a TaskInfos and ArmoniKClient.
  /// </summary>
  /// <param name="armoniKClient">The ArmoniK client for operations.</param>
  /// <param name="taskInfos">The TaskInfos to wrap.</param>
  /// <returns>A new TaskHandle instance.</returns>
  /// <exception cref="ArgumentNullException">Thrown when taskInfos or armoniKClient is null.</exception>
  public static TaskHandle FromTaskInfos(ArmoniKClient armoniKClient,
                                         TaskInfos     taskInfos)
    => new(armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient)),
           taskInfos     ?? throw new ArgumentNullException(nameof(taskInfos)),
           null);

  /// <summary>
  ///   Creates a TaskHandle from a TaskCompletionSource and ArmoniKClient.
  /// </summary>
  /// <param name="armoniKClient">The ArmoniK client for operations.</param>
  /// <param name="source">The TaskInfos's source</param>
  /// <returns>A new TaskHandle instance.</returns>
  /// <exception cref="ArgumentNullException">Thrown when source or armoniKClient is null.</exception>
  public static TaskHandle FromTaskCompletionSourceOfTaskInfos(ArmoniKClient                    armoniKClient,
                                                               TaskCompletionSource<TaskInfos>? source = null)
    => new(armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient)),
           null,
           source ?? new TaskCompletionSource<TaskInfos>());

  /// <summary>
  ///   Get the TaskInfo instance.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the TaskInfo instance</returns>
  public ValueTask<TaskInfos> GetTaskInfosAsync()
  {
    var taskInfos = taskInfos_;
    if (taskInfos is not null)
    {
      return new ValueTask<TaskInfos>(taskInfos);
    }

    return Core();

    async ValueTask<TaskInfos> Core()
    {
      var tcs = taskInfosSource_;
      if (tcs is null)
      {
        return taskInfos_!;
      }

      var taskInfos = await tcs.Task.ConfigureAwait(false);
      taskInfos_       = taskInfos;
      taskInfosSource_ = null;
      return taskInfos;
    }
  }

  /// <summary>
  ///   Asynchronously retrieves detailed state information about the task associated with this handle.
  /// </summary>
  /// <param name="cancellationToken">A token that can be used to request cancellation of the asynchronous operation.</param>
  /// <returns>
  ///   A <see cref="Task{TaskState}" /> representing the asynchronous operation, with the task's detailed state as
  ///   the result.
  /// </returns>
  public async Task<TaskState> GetTaskDetailsAsync(CancellationToken cancellationToken)
  {
    var taskInfos = await GetTaskInfosAsync()
                      .ConfigureAwait(false);
    return await ArmoniKClient.TasksService.GetTasksDetailedAsync(taskInfos.TaskId,
                                                                  cancellationToken)
                              .ConfigureAwait(false);
  }
}
