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

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extensions.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;

namespace ArmoniK.Extensions.CSharp.Client.Common.Services;

/// <summary>
///   Defines a service for managing tasks, including submission, retrieval, and cancellation of tasks.
/// </summary>
public interface ITasksService
{
  /// <summary>
  ///   Get a queryable object on TaskSummary instances
  /// </summary>
  /// <returns>An IQueryable instance to apply Linq extensions methods on</returns>
  IQueryable<TaskSummary> AsQueryable();

  /// <summary>
  ///   Asynchronously submits a collection of tasks for a given session.
  /// </summary>
  /// <param name="session">The session information to which the tasks belong.</param>
  /// <param name="taskDefinitions">The task definitions.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of task infos.</returns>
  IAsyncEnumerable<TaskInfos> SubmitTasksAsync(SessionInfo                 session,
                                               ICollection<TaskDefinition> taskDefinitions,
                                               CancellationToken           cancellationToken = default);

  /// <summary>
  ///   Asynchronously lists tasks based on pagination options.
  /// </summary>
  /// <param name="paginationOptions">The options for pagination, including page number, page size, and sorting.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains a task page..</returns>
  Task<TaskPage> ListTasksAsync(TaskPagination    paginationOptions,
                                CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously retrieves detailed information about a specific task.
  /// </summary>
  /// <param name="taskInfos">The task's TaskInfos to retrieve details for.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the detailed task state.</returns>
  Task<TaskState> GetTasksDetailedAsync(TaskInfos         taskInfos,
                                        CancellationToken cancellationToken = default);

  /// <summary>
  ///   Asynchronously cancels a collection of tasks based on their identifiers.
  /// </summary>
  /// <param name="taskIds">The identifiers of the tasks to cancel.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of detailed task states.</returns>
  IAsyncEnumerable<TaskSummary> CancelTasksAsync(IEnumerable<string> taskIds,
                                                 CancellationToken   cancellationToken = default);
}
