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
using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Partition;
using ArmoniK.Extensions.CSharp.Client.Common.Enum;

namespace Tests.Queryable;

public class BasePartitionFilterTests
{
  private static readonly Dictionary<string, PartitionRawEnumField> MemberName2EnumField_ = new()
                                                                                            {
                                                                                              {
                                                                                                nameof(Partition.PartitionId), PartitionRawEnumField.Id
                                                                                              },
                                                                                              {
                                                                                                nameof(Partition.PodMax), PartitionRawEnumField.PodMax
                                                                                              },
                                                                                              {
                                                                                                nameof(Partition.PodReserved), PartitionRawEnumField.PodReserved
                                                                                              },
                                                                                              {
                                                                                                nameof(Partition.PreemptionPercentage),
                                                                                                PartitionRawEnumField.PreemptionPercentage
                                                                                              },
                                                                                              {
                                                                                                nameof(Partition.Priority), PartitionRawEnumField.Priority
                                                                                              },
                                                                                              {
                                                                                                nameof(Partition.ParentPartitionIds),
                                                                                                PartitionRawEnumField.ParentPartitionIds
                                                                                              },
                                                                                            };

  private static readonly Dictionary<string, FilterStringOperator> Op2EnumStringOp = new()
                                                                                     {
                                                                                       {
                                                                                         "==", FilterStringOperator.Equal
                                                                                       },
                                                                                       {
                                                                                         "!=", FilterStringOperator.NotEqual
                                                                                       },
                                                                                       {
                                                                                         "Contains", FilterStringOperator.Contains
                                                                                       },
                                                                                       {
                                                                                         "NotContains", FilterStringOperator.NotContains
                                                                                       },
                                                                                       {
                                                                                         "StartsWith", FilterStringOperator.StartsWith
                                                                                       },
                                                                                       {
                                                                                         "EndsWith", FilterStringOperator.EndsWith
                                                                                       },
                                                                                     };

  private static readonly Dictionary<string, FilterNumberOperator> Op2EnumIntOp = new()
                                                                                  {
                                                                                    {
                                                                                      "==", FilterNumberOperator.Equal
                                                                                    },
                                                                                    {
                                                                                      "!=", FilterNumberOperator.NotEqual
                                                                                    },
                                                                                    {
                                                                                      "<", FilterNumberOperator.LessThan
                                                                                    },
                                                                                    {
                                                                                      "<=", FilterNumberOperator.LessThanOrEqual
                                                                                    },
                                                                                    {
                                                                                      ">", FilterNumberOperator.GreaterThan
                                                                                    },
                                                                                    {
                                                                                      ">=", FilterNumberOperator.GreaterThanOrEqual
                                                                                    },
                                                                                  };

  private static readonly Dictionary<string, FilterStatusOperator> Op2EnumStatusOp = new()
                                                                                     {
                                                                                       {
                                                                                         "==", FilterStatusOperator.Equal
                                                                                       },
                                                                                       {
                                                                                         "!=", FilterStatusOperator.NotEqual
                                                                                       },
                                                                                     };

  private static readonly Dictionary<string, FilterDateOperator> Op2EnumDateOp = new()
                                                                                 {
                                                                                   {
                                                                                     "==", FilterDateOperator.Equal
                                                                                   },
                                                                                   {
                                                                                     "!=", FilterDateOperator.NotEqual
                                                                                   },
                                                                                   {
                                                                                     "<", FilterDateOperator.Before
                                                                                   },
                                                                                   {
                                                                                     "<=", FilterDateOperator.BeforeOrEqual
                                                                                   },
                                                                                   {
                                                                                     ">", FilterDateOperator.After
                                                                                   },
                                                                                   {
                                                                                     ">=", FilterDateOperator.AfterOrEqual
                                                                                   },
                                                                                 };

  private static readonly Dictionary<string, FilterArrayOperator> Op2EnumArrayOp = new()
                                                                                   {
                                                                                     {
                                                                                       "Contains", FilterArrayOperator.Contains
                                                                                     },
                                                                                     {
                                                                                       "NotContains", FilterArrayOperator.NotContains
                                                                                     },
                                                                                   };

  protected PartitionPagination BuildPartitionPagination(Filters filter,
                                                         string  sortCriteria  = null!,
                                                         bool    ascendingSort = true)
    => new()
       {
         Filter   = filter,
         Page     = 0,
         PageSize = 1000,
         SortDirection = ascendingSort
                           ? SortDirection.Asc
                           : SortDirection.Desc,
         SortField = new PartitionField
                     {
                       PartitionRawField = new PartitionRawField
                                           {
                                             Field = string.IsNullOrEmpty(sortCriteria)
                                                       ? PartitionRawEnumField.Id
                                                       : MemberName2EnumField_[sortCriteria],
                                           },
                     },
       };

  private PartitionField BuildPartitionField(string fieldname)
    => new()
       {
         PartitionRawField = new PartitionRawField
                             {
                               Field = MemberName2EnumField_[fieldname],
                             },
       };

  protected FilterField BuildFilterString(string fieldName,
                                          string op,
                                          string value)
    => new()
       {
         Field = BuildPartitionField(fieldName),
         FilterString = new FilterString
                        {
                          Operator = Op2EnumStringOp[op],
                          Value    = value,
                        },
       };

  protected FilterField BuildFilterInt(string fieldName,
                                       string op,
                                       int    value)
    => new()
       {
         Field = BuildPartitionField(fieldName),
         FilterNumber = new FilterNumber
                        {
                          Operator = Op2EnumIntOp[op],
                          Value    = value,
                        },
       };

  protected FilterField BuildFilterArray(string fieldName,
                                         string op,
                                         string val)
    => new()
       {
         Field = BuildPartitionField(fieldName),
         FilterArray = new FilterArray
                       {
                         Operator = Op2EnumArrayOp[op],
                         Value    = val,
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
