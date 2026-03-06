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
  ///   The ID of the Task that as submitted this task if any.
  /// </summary>
  public string? CreatedBy { get; init; } = string.Empty;

  /// <summary>
  ///   Time when the task was created.
  /// </summary>
  public DateTime CreatedAt { get; init; }

  /// <summary>
  ///   The task submission date.
  /// </summary>
  public DateTime SubmittedAt { get; init; }

  /// <summary>
  ///   When the task is received by the agent.
  /// </summary>
  public DateTime? ReceivedAt { get; init; }

  /// <summary>
  ///   When the task is acquired by the agent.
  /// </summary>
  public DateTime? AcquiredAt { get; init; }

  /// <summary>
  ///   Task data retrieval end date.
  /// </summary>
  public DateTime? FetchedAt { get; init; }

  /// <summary>
  ///   The end of task processing date.
  /// </summary>
  public DateTime? ProcessedAt { get; init; }

  /// <summary>
  ///   The pod Time To Live.
  /// </summary>
  public DateTime? PodTTL { get; init; }

  /// <summary>
  ///   Time when the task ended.
  /// </summary>
  public DateTime? EndedAt { get; init; }

  /// <summary>
  ///   Time when the task started.
  /// </summary>
  public DateTime? StartedAt { get; init; }

  /// <summary>
  ///   The task duration. Between the creation date and the end date.
  /// </summary>
  public TimeSpan? CreationToEnd { get; init; }

  /// <summary>
  ///   The task calculated duration. Between the start date and the end date.
  /// </summary>
  public TimeSpan? ProcessingToEnd { get; init; }

  /// <summary>
  ///   The task calculated duration. Between the received date and the end date.
  /// </summary>
  public TimeSpan? ReceivedToEnd { get; init; }

  /// <summary>
  ///   Current status of the task.
  /// </summary>
  public TaskStatus Status { get; init; }

  /// <summary>
  ///   The owner pod ID.
  /// </summary>
  public string OwnerPodId { get; init; } = string.Empty;

  /// <summary>
  ///   The hostname of the container running the task.
  /// </summary>
  public string PodHostName { get; init; } = string.Empty;

  /// <summary>
  ///   The initial task ID. Set when a task is submitted independently of retries.
  /// </summary>
  public string InitialTaskId { get; init; } = string.Empty;
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
         CreatedBy             = taskSummary.CreatedBy,
         CreatedAt             = taskSummary.CreatedAt.ToDateTime(),
         SubmittedAt           = taskSummary.SubmittedAt.ToDateTime(),
         ReceivedAt            = taskSummary.ReceivedAt?.ToDateTime(),
         AcquiredAt            = taskSummary.AcquiredAt?.ToDateTime(),
         FetchedAt             = taskSummary.FetchedAt?.ToDateTime(),
         ProcessedAt           = taskSummary.ProcessedAt?.ToDateTime(),
         PodTTL                = taskSummary.PodTtl?.ToDateTime(),
         StartedAt             = taskSummary.StartedAt?.ToDateTime(),
         CreationToEnd         = taskSummary.CreationToEndDuration?.ToTimeSpan(),
         ProcessingToEnd       = taskSummary.ProcessingToEndDuration?.ToTimeSpan(),
         ReceivedToEnd         = taskSummary.ReceivedToEndDuration?.ToTimeSpan(),
         EndedAt               = taskSummary.EndedAt?.ToDateTime(),
         SessionId             = taskSummary.SessionId,
         PayloadId             = taskSummary.PayloadId,
         OwnerPodId            = taskSummary.OwnerPodId,
         PodHostName           = taskSummary.PodHostname,
         InitialTaskId         = taskSummary.InitialTaskId,
       };
}
