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
using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Partition;

using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using Type = System.Type;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.PartitionQuery;

/// <summary>
///   Specialisation of WhereExpressionTreeVisitor for queries on Partition instances.
/// </summary>
internal class PartitionWhereExpressionTreeVisitor : WhereExpressionTreeVisitor<PartitionField, Filters, FiltersAnd, FilterField>
{
  protected override FiltersAnd CreateFilterAnd(params FilterField[] filters)
  {
    var andFilter = new FiltersAnd();
    andFilter.And.Add(filters);
    return andFilter;
  }

  protected override Filters CreateFilterOr(params FiltersAnd[] filters)
  {
    var orFilter = new Filters();
    orFilter.Or.Add(filters);
    return orFilter;
  }

  protected override FilterField CreateDateFilter(PartitionField     field,
                                                  FilterDateOperator op,
                                                  Timestamp          val)
    // should never happen since there is no Date property in Partition
    => throw new InvalidOperationException("Invalid filter: Date filter is not supported for Partition queries.");

  protected override FilterField CreateDurationFilter(PartitionField         field,
                                                      FilterDurationOperator op,
                                                      Duration               val)
    // should never happen since there is no Duration property in Partition
    => throw new InvalidOperationException("Invalid filter: Duration filter is not supported for Partition queries.");

  protected override FilterField CreateNumberFilter(PartitionField       field,
                                                    FilterNumberOperator op,
                                                    long                 val)
  {
    var filterField = new FilterField
                      {
                        FilterNumber = new FilterNumber
                                       {
                                         Operator = op,
                                         Value    = val,
                                       },
                        Field = field,
                      };
    return filterField;
  }

  protected override FilterField CreateStringFilter(PartitionField       field,
                                                    FilterStringOperator op,
                                                    string               val)
  {
    var filterField = new FilterField
                      {
                        FilterString = new FilterString
                                       {
                                         Operator = op,
                                         Value    = val,
                                       },
                        Field = field,
                      };
    return filterField;
  }

  protected override FilterField CreateTaskStatusFilter(PartitionField       field,
                                                        FilterStatusOperator op,
                                                        TaskStatus           val)
    // should never happen since there is no TaskStatus property in Partition
    => throw new InvalidOperationException("Invalid filter: TaskStatus filter is not supported for Partition queries.");

  protected override FilterField CreateBlobStatusFilter(PartitionField       field,
                                                        FilterStatusOperator op,
                                                        ResultStatus         val)
    // should never happen since there is no BlobStatus property in Partition
    => throw new InvalidOperationException("Invalid filter: BlobStatus filter is not supported for Partition queries.");

  protected override FilterField CreateArrayFilter(PartitionField      field,
                                                   FilterArrayOperator op,
                                                   string              val)
  {
    var filterField = new FilterField
                      {
                        FilterArray = new FilterArray
                                      {
                                        Operator = op,
                                        Value    = val,
                                      },
                        Field = field,
                      };
    return filterField;
  }

  protected override RepeatedField<FiltersAnd> GetRepeatedFilterAnd(Filters or)
    => or.Or;

  protected override RepeatedField<FilterField> GetRepeatedFilterField(FiltersAnd and)
    => and.And;

  protected override void OnCollectionContains(PartitionField      field,
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

  protected override void OnIndexerAccess()
    // That case never happen for Partition instances and is not supported
    => throw new InvalidOperationException("Invalid filter expression.");

  protected override bool PushProperty(MemberExpression member)
  {
    if (PartitionMaps.MemberName2Type_.TryGetValue(member.Member.Name,
                                     out var memberType) && PartitionMaps.MemberName2EnumField_.TryGetValue(member.Member.Name,
                                                                                              out var enumField))
    {
      var field = new PartitionField
                  {
                    PartitionRawField = new PartitionRawField
                                        {
                                          Field = enumField,
                                        },
                  };
      FilterStack.Push((field, memberType));
      return true;
    }

    return false;
  }
}
