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

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;
using TaskSummary = ArmoniK.Extensions.CSharp.Client.Common.Domain.Task.TaskSummary;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.TaskSummaryQuery;

/// <summary>
///   Specialisation of ArmoniKQueryProvider for queries on TaskState instances.
/// </summary>
internal class TaskSummaryQueryProvider : ArmoniKQueryProvider<ITasksService, TaskPage, TaskSummary, TaskField, Filters, FiltersAnd, FilterField>
{
  private readonly ITasksService tasksService_;

  public TaskSummaryQueryProvider(ITasksService          service,
                                  ILogger<ITasksService> logger)
    : base(logger)
    => tasksService_ = service;

  protected override QueryExecution<TaskPage, TaskSummary, TaskField, Filters, FiltersAnd, FilterField> CreateQueryExecution()
    => new TaskSummaryQueryExecution(tasksService_,
                                     Logger);
}
