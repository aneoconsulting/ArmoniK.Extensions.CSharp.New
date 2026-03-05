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

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Client.Common.Enum;
using ArmoniK.Extensions.CSharp.Client.Queryable.TaskSummaryQuery;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;

using Google.Protobuf.WellKnownTypes;

using TaskStatus = ArmoniK.Extensions.CSharp.Common.Common.Domain.Task.TaskStatus;

namespace Tests.Queryable;

public class BaseTaskFilterTests
{
  protected TaskPagination BuildTaskPagination(Filters    filter,
                                               TaskField? sortCriteria       = null,
                                               bool       ascendingSort      = true,
                                               bool       useDetailedVersion = false)
    => new()
       {
         UseDetailedVersion = useDetailedVersion,
         Filter             = filter,
         Page               = 0,
         PageSize           = 1000,
         SortDirection = ascendingSort
                           ? SortDirection.Asc
                           : SortDirection.Desc,
         SortField = sortCriteria ?? new TaskField
                                     {
                                       TaskSummaryField = new TaskSummaryField
                                                          {
                                                            Field = TaskSummaryEnumField.TaskId,
                                                          },
                                     },
       };

  protected TaskField BuildTaskSummaryOrder(string sortCriteria)
    => new()
       {
         TaskSummaryField = new TaskSummaryField
                            {
                              Field = TaskSummaryMaps.MemberName2EnumField_[sortCriteria],
                            },
       };

  protected TaskField BuildTaskOptionOrder(string sortCriteria)
    => new()
       {
         TaskOptionField = new TaskOptionField
                           {
                             Field = TaskSummaryMaps.MemberName2OptionEnumField_[sortCriteria],
                           },
       };

  protected TaskField BuildTaskOptionGenericOrder(string sortKey)
    => new()
       {
         TaskOptionGenericField = new TaskOptionGenericField
                                  {
                                    Field = sortKey,
                                  },
       };

  private TaskField BuildTaskField(string  fieldName,
                                   string? key = null)
  {
    if (fieldName == nameof(TaskState.TaskOptions.Options) && !string.IsNullOrEmpty(key))
    {
      return new TaskField
             {
               TaskOptionGenericField = new TaskOptionGenericField
                                        {
                                          Field = key,
                                        },
             };
    }

    if (TaskSummaryMaps.MemberName2EnumField_.TryGetValue(fieldName,
                                                          out var fieldSummary))
    {
      return new TaskField
             {
               TaskSummaryField = new TaskSummaryField
                                  {
                                    Field = fieldSummary,
                                  },
             };
    }

    return new TaskField
           {
             TaskOptionField = new TaskOptionField
                               {
                                 Field = TaskSummaryMaps.MemberName2OptionEnumField_[fieldName],
                               },
           };
  }

  protected FilterField BuildFilterString(string fieldName,
                                          string op,
                                          string value)
  {
    var elements = fieldName.Split(':');
    if (elements.Length == 2)
    {
      return new FilterField
             {
               Field = BuildTaskField(elements[0],
                                      elements[1]),
               FilterString = new FilterString
                              {
                                Operator = FiltersMaps.Op2EnumStringOp_[op],
                                Value    = value,
                              },
             };
    }

    return new FilterField
           {
             Field = BuildTaskField(fieldName),
             FilterString = new FilterString
                            {
                              Operator = FiltersMaps.Op2EnumStringOp_[op],
                              Value    = value,
                            },
           };
  }

  protected FilterField BuildFilterInt(string fieldName,
                                       string op,
                                       int    value)
    => new()
       {
         Field = BuildTaskField(fieldName),
         FilterNumber = new FilterNumber
                        {
                          Operator = FiltersMaps.Op2EnumIntOp_[op],
                          Value    = value,
                        },
       };

  protected FilterField BuildFilterStatus(string     fieldName,
                                          string     op,
                                          TaskStatus value)
    => new()
       {
         Field = BuildTaskField(fieldName),
         FilterStatus = new FilterStatus
                        {
                          Operator = FiltersMaps.Op2EnumStatusOp_[op],
                          Value    = value.ToGrpcStatus(),
                        },
       };

  protected FilterField BuildFilterDateTime(string   fieldName,
                                            string   op,
                                            DateTime value)
    => new()
       {
         Field = BuildTaskField(fieldName),
         FilterDate = new FilterDate
                      {
                        Operator = FiltersMaps.Op2EnumDateOp_[op],
                        Value = value.ToUniversalTime()
                                     .ToTimestamp(),
                      },
       };

  protected FilterField BuildFilterDuration(string   fieldName,
                                            string   op,
                                            TimeSpan value)
    => new()
       {
         Field = BuildTaskField(fieldName),
         FilterDuration = new FilterDuration
                          {
                            Operator = FiltersMaps.Op2EnumDurationOp_[op],
                            Value    = Duration.FromTimeSpan(value),
                          },
       };

  protected FiltersAnd BuildAnd(params FilterField[] filters)
  {
    var filterAnd = new FiltersAnd();
    foreach (var filter in filters)
    {
      filterAnd.And.Add(filter);
    }

    return filterAnd;
  }

  protected Filters BuildOr(params FiltersAnd[] filtersAnd)
  {
    var filterOr = new Filters();
    foreach (var filterAnd in filtersAnd)
    {
      filterOr.Or.Add(filterAnd);
    }

    return filterOr;
  }
}
