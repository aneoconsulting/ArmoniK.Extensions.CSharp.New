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
using System.Collections.Generic;

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;

using TaskSummary = ArmoniK.Extensions.CSharp.Client.Common.Domain.Task.TaskSummary;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.TaskSummaryQuery;

internal static class TaskSummaryMaps
{
  public static readonly Dictionary<string, TaskSummaryEnumField> MemberName2EnumField_ = new()
                                                                                          {
                                                                                            {
                                                                                              nameof(TaskSummary.TaskId), TaskSummaryEnumField.TaskId
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.PayloadId), TaskSummaryEnumField.PayloadId
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.SessionId), TaskSummaryEnumField.SessionId
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.CreatedBy), TaskSummaryEnumField.CreatedBy
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.CreatedAt), TaskSummaryEnumField.CreatedAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.SubmittedAt), TaskSummaryEnumField.SubmittedAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.AcquiredAt), TaskSummaryEnumField.AcquiredAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.FetchedAt), TaskSummaryEnumField.FetchedAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.ProcessedAt), TaskSummaryEnumField.ProcessedAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.PodTTL), TaskSummaryEnumField.PodTtl
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.ReceivedAt), TaskSummaryEnumField.ReceivedAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.EndedAt), TaskSummaryEnumField.EndedAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.StartedAt), TaskSummaryEnumField.StartedAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.CreationToEnd),
                                                                                              TaskSummaryEnumField.CreationToEndDuration
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.ProcessingToEnd),
                                                                                              TaskSummaryEnumField.ProcessingToEndDuration
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.ReceivedToEnd),
                                                                                              TaskSummaryEnumField.ReceivedToEndDuration
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.Status), TaskSummaryEnumField.Status
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.OwnerPodId), TaskSummaryEnumField.OwnerPodId
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.PodHostName), TaskSummaryEnumField.PodHostname
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskSummary.InitialTaskId), TaskSummaryEnumField.InitialTaskId
                                                                                            },
                                                                                          };

  public static readonly Dictionary<string, TaskOptionEnumField> MemberName2OptionEnumField_ = new()
                                                                                               {
                                                                                                 {
                                                                                                   nameof(TaskConfiguration.MaxDuration), TaskOptionEnumField.MaxDuration
                                                                                                 },
                                                                                                 {
                                                                                                   nameof(TaskConfiguration.MaxRetries), TaskOptionEnumField.MaxRetries
                                                                                                 },
                                                                                                 {
                                                                                                   nameof(TaskConfiguration.PartitionId), TaskOptionEnumField.PartitionId
                                                                                                 },
                                                                                                 {
                                                                                                   nameof(TaskConfiguration.Priority), TaskOptionEnumField.Priority
                                                                                                 },
                                                                                               };

  public static readonly Dictionary<string, Type> MemberName2Type_ = new()
                                                                     {
                                                                       {
                                                                         nameof(TaskSummary.TaskId), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.PayloadId), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.SessionId), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.CreatedBy), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.CreatedAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.SubmittedAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.AcquiredAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.FetchedAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.ProcessedAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.PodTTL), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.ReceivedAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.EndedAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.StartedAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.CreationToEnd), typeof(TimeSpan)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.ProcessingToEnd), typeof(TimeSpan)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.ReceivedToEnd), typeof(TimeSpan)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.Status), typeof(TaskStatus)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.OwnerPodId), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.PodHostName), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskSummary.InitialTaskId), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskConfiguration.MaxDuration), typeof(TimeSpan)
                                                                       },
                                                                       {
                                                                         nameof(TaskConfiguration.MaxRetries), typeof(int)
                                                                       },
                                                                       {
                                                                         nameof(TaskConfiguration.PartitionId), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskConfiguration.Priority), typeof(int)
                                                                       },
                                                                     };
}
