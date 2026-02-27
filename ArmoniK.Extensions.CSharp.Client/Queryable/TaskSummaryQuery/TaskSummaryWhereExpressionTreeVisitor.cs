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
using System.Data;
using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;

using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using FilterField = ArmoniK.Api.gRPC.V1.Tasks.FilterField;
using Filters = ArmoniK.Api.gRPC.V1.Tasks.Filters;
using FiltersAnd = ArmoniK.Api.gRPC.V1.Tasks.FiltersAnd;
using FilterStatus = ArmoniK.Api.gRPC.V1.Tasks.FilterStatus;
using TaskStatus = ArmoniK.Extensions.CSharp.Common.Common.Domain.Task.TaskStatus;
using Type = System.Type;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.TaskStateQuery;

/// <summary>
///   Specialisation of WhereExpressionTreeVisitor for queries on TaskState instances.
/// </summary>
internal class TaskSummaryWhereExpressionTreeVisitor : WhereExpressionTreeVisitor<TaskField, Filters, FiltersAnd, FilterField>
{
  private static readonly Dictionary<string, TaskSummaryEnumField> MemberName2EnumField_ = new()
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

  private static readonly Dictionary<string, TaskOptionEnumField> MemberName2OptionEnumField_ = new()
                                                                                                {
                                                                                                  {
                                                                                                    nameof(TaskConfiguration.MaxDuration),
                                                                                                    TaskOptionEnumField.MaxDuration
                                                                                                  },
                                                                                                  {
                                                                                                    nameof(TaskConfiguration.MaxRetries), TaskOptionEnumField.MaxRetries
                                                                                                  },
                                                                                                  {
                                                                                                    nameof(TaskConfiguration.PartitionId),
                                                                                                    TaskOptionEnumField.PartitionId
                                                                                                  },
                                                                                                  {
                                                                                                    nameof(TaskConfiguration.Priority), TaskOptionEnumField.Priority
                                                                                                  },
                                                                                                };

  private static readonly Dictionary<string, Type> MemberName2Type_ = new()
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

  protected override Filters CreateFilterOr(params FiltersAnd[] filters)
  {
    var orFilter = new Filters();
    orFilter.Or.Add(filters);
    return orFilter;
  }

  protected override FiltersAnd CreateFilterAnd(params FilterField[] filters)
  {
    var andFilter = new FiltersAnd();
    andFilter.And.Add(filters);
    return andFilter;
  }

  protected override RepeatedField<FiltersAnd> GetRepeatedFilterAnd(Filters or)
    => or.Or;

  protected override RepeatedField<FilterField> GetRepeatedFilterField(FiltersAnd and)
    => and.And;

  protected override bool PushProperty(MemberExpression member)
  {
    Type memberType;
    if (member.Expression.Type == typeof(TaskConfiguration))
    {
      if (MemberName2Type_.TryGetValue(member.Member.Name,
                                       out memberType) && MemberName2OptionEnumField_.TryGetValue(member.Member.Name,
                                                                                                  out var enumOptionField))
      {
        // filter on a property of TaskOption property
        var taskField = new TaskField
                        {
                          TaskOptionField = new TaskOptionField
                                            {
                                              Field = enumOptionField,
                                            },
                        };
        FilterStack.Push((taskField, memberType));
        return true;
      }

      if (member.Member.Name == nameof(TaskConfiguration.Options))
      {
        var taskField = new TaskField
                        {
                          TaskOptionGenericField = new TaskOptionGenericField(), // value must be set later by OnIndexerAccess()
                        };
        FilterStack.Push((taskField, typeof(string)));
        return true;
      }
    }

    if (MemberName2Type_.TryGetValue(member.Member.Name,
                                     out memberType) && MemberName2EnumField_.TryGetValue(member.Member.Name,
                                                                                          out var enumField))
    {
      var taskField = new TaskField
                      {
                        TaskSummaryField = new TaskSummaryField
                                           {
                                             Field = enumField,
                                           },
                      };
      FilterStack.Push((taskField, memberType));
      return true;
    }

    return false;
  }

  protected override void OnIndexerAccess()
  {
    var (rhs, _) = FilterStack.Pop();
    var (lhs, _) = FilterStack.Pop();

    if (lhs is TaskField taskField && taskField.TaskOptionGenericField is not null && rhs is string key)
    {
      taskField.TaskOptionGenericField.Field = key;
      FilterStack.Push((taskField, typeof(string)));
    }
    else
    {
      throw new InvalidExpressionException("Invalid filter expression");
    }
  }

  protected override FilterField CreateStringFilter(TaskField            taskField,
                                                    FilterStringOperator op,
                                                    string               val)
  {
    var filterField = new FilterField();
    filterField.FilterString = new FilterString
                               {
                                 Operator = op,
                                 Value    = val,
                               };
    filterField.Field = taskField;
    return filterField;
  }

  protected override FilterField CreateNumberFilter(TaskField            taskField,
                                                    FilterNumberOperator op,
                                                    int                  val)
  {
    var filterField = new FilterField();
    filterField.FilterNumber = new FilterNumber
                               {
                                 Operator = op,
                                 Value    = val,
                               };
    filterField.Field = taskField;
    return filterField;
  }

  protected override FilterField CreateDateFilter(TaskField          taskField,
                                                  FilterDateOperator op,
                                                  Timestamp          val)
  {
    var filterField = new FilterField();
    filterField.FilterDate = new FilterDate
                             {
                               Operator = op,
                               Value    = val,
                             };
    filterField.Field = taskField;
    return filterField;
  }

  protected override FilterField CreateDurationFilter(TaskField              taskField,
                                                      FilterDurationOperator op,
                                                      Duration               val)
  {
    var filterField = new FilterField();
    filterField.FilterDuration = new FilterDuration
                                 {
                                   Operator = op,
                                   Value    = val,
                                 };
    filterField.Field = taskField;
    return filterField;
  }

  protected override FilterField CreateTaskStatusFilter(TaskField              taskField,
                                                        FilterStatusOperator   op,
                                                        Api.gRPC.V1.TaskStatus val)
  {
    var filterField = new FilterField();
    filterField.FilterStatus = new FilterStatus
                               {
                                 Operator = op,
                                 Value    = val,
                               };
    filterField.Field = taskField;
    return filterField;
  }

  protected override FilterField CreateBlobStatusFilter(TaskField            field,
                                                        FilterStatusOperator op,
                                                        ResultStatus         val)
    // should never happen since there is no BlobStatus property in TaskState
    => throw new InvalidOperationException("Invalid filter: BlobStatus filter is not supported for TaskState queries.");

  protected override void OnCollectionContains(TaskField           field,
                                               IEnumerable<object> collection,
                                               bool                notOp = false)
  {
    var isEmpty = true;
    var orNode  = new Filters();
    if (notOp)
    {
      orNode.Or.Add(new FiltersAnd());
    }

    foreach (var val in collection)
    {
      isEmpty = false;
      if (notOp)
      {
        orNode.Or[0]
              .And.Add(CreateFilterField(field,
                                         val,
                                         false));
      }
      else
      {
        orNode.Or.Add(new FiltersAnd
                      {
                        And =
                        {
                          CreateFilterField(field,
                                            val,
                                            true),
                        },
                      });
      }
    }

    if (isEmpty)
    {
      FilterStack.Push((notOp, typeof(bool)));
    }
    else
    {
      FilterStack.Push((orNode, typeof(bool)));
    }
  }
}
