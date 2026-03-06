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
using ArmoniK.Extensions.CSharp.Client.Queryable.PartitionQuery;

namespace Tests.Queryable;

public class BasePartitionFilterTests
{
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
                                                       : PartitionMaps.MemberName2EnumField_[sortCriteria],
                                           },
                     },
       };

  private PartitionField BuildPartitionField(string fieldname)
    => new()
       {
         PartitionRawField = new PartitionRawField
                             {
                               Field = PartitionMaps.MemberName2EnumField_[fieldname],
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
                          Operator = FiltersMaps.Op2EnumStringOp_[op],
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
                          Operator = FiltersMaps.Op2EnumIntOp_[op],
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
                         Operator = FiltersMaps.Op2EnumArrayOp_[op],
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
