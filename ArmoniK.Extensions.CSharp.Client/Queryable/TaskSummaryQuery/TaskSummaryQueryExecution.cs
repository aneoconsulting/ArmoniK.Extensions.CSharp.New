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

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Client.Common.Enum;
using ArmoniK.Extensions.CSharp.Client.Common.Generic;
using ArmoniK.Extensions.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;
using TaskSummary = ArmoniK.Extensions.CSharp.Client.Common.Domain.Task.TaskSummary;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.TaskStateQuery;

/// <summary>
///   Specialisation of QueryExecution for queries on TaskState instances.
/// </summary>
internal class TaskSummaryQueryExecution : QueryExecution<TaskPage, TaskSummary, TaskField, Filters, FiltersAnd, FilterField>
{
  private readonly ILogger<ITasksService> logger_;
  private readonly ITasksService          tasksService_;

  public TaskSummaryQueryExecution(ITasksService          service,
                                   ILogger<ITasksService> logger)
  {
    tasksService_ = service;
    logger_       = logger;
  }

  protected override void LogError(Exception ex,
                                   string    message)
    => logger_.LogError(ex,
                        message);

  protected override async Task<TaskPage> RequestInstancesAsync(Pagination<Filters, TaskField> pagination,
                                                                CancellationToken              cancellationToken)
    => await tasksService_.ListTasksAsync((TaskPagination)pagination,
                                          cancellationToken)
                          .ConfigureAwait(false);

  protected override QueryExpressionTreeVisitor<TaskSummary, TaskField, Filters, FiltersAnd, FilterField> CreateQueryExpressionTreeVisitor()
    => new TaskSummaryQueryExpressionTreeVisitor();

  protected override Pagination<Filters, TaskField> CreatePaginationInstance(
    QueryExpressionTreeVisitor<TaskSummary, TaskField, Filters, FiltersAnd, FilterField> visitor)
  {
    var useDetailedVersion = ((TaskSummaryQueryExpressionTreeVisitor)visitor).UseDetailedVersion;
    return new TaskPagination
           {
             UseDetailedVersion = useDetailedVersion,
             Filter             = visitor.Filters!,
             Page               = 0,
             PageSize = visitor.PageSize.HasValue
                          ? visitor.PageSize.Value
                          : 1000,
             SortDirection = visitor.IsSortAscending
                               ? SortDirection.Asc
                               : SortDirection.Desc,
             SortField = visitor.SortCriteria,
           };
  }

  protected override int GetTotalPageElements(TaskPage page)
    => page.TotalTasks;

  protected override object[] GetPageElements(Pagination<Filters, TaskField> pagination,
                                              TaskPage                       page)
  {
    var taskPagination = (TaskPagination)pagination;
    if (taskPagination.UseDetailedVersion)
    {
      return page.TaskDetails!;
    }

    return page.TasksSummaries!;
  }
}
