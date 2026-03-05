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

using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;

namespace ArmoniK.Extensions.CSharp.Client.Common.Domain.Task;

/// <summary>
///   Represents a simplified state of a task.
///   Its purpose is to be used for tasks requesting and filtering.
/// </summary>
public class TaskSummary
{
  /// <summary>
  ///   Identifier of the task.
  /// </summary>
  public string TaskId { get; init; } = string.Empty;

  /// <summary>
  ///   Collection of expected output IDs.
  /// </summary>
  public long ExpectedOutputsCount { get; init; }

  /// <summary>
  ///   Collection of data dependencies IDs.
  /// </summary>
  public long DataDependenciesCount { get; init; }

  /// <summary>
  ///   Identifier for the payload associated with the task.
  /// </summary>
  public string PayloadId { get; init; } = string.Empty;

  /// <summary>
  ///   Session ID associated with the task.
  /// </summary>
  public string SessionId { get; init; } = string.Empty;

  /// <summary>
  ///   Gets the configuration options for the task, which may be null if no additional configurations are specified.
  /// </summary>
  public TaskConfiguration TaskOptions { get; set; } = new();

  /// <summary>
  ///   Time when the task was created.
  /// </summary>
  public DateTime CreateAt { get; init; }

  /// <summary>
  ///   Time when the task ended.
  /// </summary>
  public DateTime? EndedAt { get; init; }

  /// <summary>
  ///   Time when the task started.
  /// </summary>
  public DateTime? StartedAt { get; init; }

  /// <summary>
  ///   Current status of the task.
  /// </summary>
  public TaskStatus Status { get; init; }
}

/// <summary>
///   Class of extensions methods to convert Protobuf instances into TaskSummary instances
/// </summary>
public static class TaskSummaryExt
{
  /// <summary>
  ///   Convert a protobuf TaskSummary instance into a SDK TaskSummary instance
  /// </summary>
  /// <param name="taskSummary">The protobuf TaskSummary instance</param>
  /// <returns>The SDK TaskSummary instance</returns>
  public static TaskSummary ToTaskSummary(this Api.gRPC.V1.Tasks.TaskSummary taskSummary)
    => new()
       {
         DataDependenciesCount = taskSummary.CountDataDependencies,
         ExpectedOutputsCount  = taskSummary.CountExpectedOutputIds,
         TaskId                = taskSummary.Id,
         Status                = taskSummary.Status.ToInternalStatus(),
         CreateAt              = taskSummary.CreatedAt.ToDateTime(),
         StartedAt             = taskSummary.StartedAt?.ToDateTime(),
         EndedAt               = taskSummary.EndedAt?.ToDateTime(),
         SessionId             = taskSummary.SessionId,
         PayloadId             = taskSummary.PayloadId,
       };
}
