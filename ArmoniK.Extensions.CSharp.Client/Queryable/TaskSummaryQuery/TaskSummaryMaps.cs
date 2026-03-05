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

namespace ArmoniK.Extensions.CSharp.Client.Queryable.TaskSummaryQuery;

internal static class TaskSummaryMaps
{
  public static readonly Dictionary<string, TaskSummaryEnumField> MemberName2EnumField_ = new()
                                                                                          {
                                                                                            {
                                                                                              nameof(TaskInfos.TaskId), TaskSummaryEnumField.TaskId
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskInfos.PayloadId), TaskSummaryEnumField.PayloadId
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskInfos.SessionId), TaskSummaryEnumField.SessionId
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskState.CreateAt), TaskSummaryEnumField.CreatedAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskState.EndedAt), TaskSummaryEnumField.EndedAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskState.StartedAt), TaskSummaryEnumField.StartedAt
                                                                                            },
                                                                                            {
                                                                                              nameof(TaskState.Status), TaskSummaryEnumField.Status
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
                                                                         nameof(TaskInfos.TaskId), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskInfos.PayloadId), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskInfos.SessionId), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(TaskState.CreateAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskState.EndedAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskState.StartedAt), typeof(DateTime)
                                                                       },
                                                                       {
                                                                         nameof(TaskState.Status), typeof(TaskStatus)
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
