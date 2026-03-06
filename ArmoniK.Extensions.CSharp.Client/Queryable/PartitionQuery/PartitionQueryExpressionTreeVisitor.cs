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

using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Partition;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.PartitionQuery;

/// <summary>
///   Specialisation of QueryExpressionTreeVisitor for queries on Partition instances.
/// </summary>
internal class PartitionQueryExpressionTreeVisitor : QueryExpressionTreeVisitor<Partition, PartitionField, Filters, FiltersAnd, FilterField>
{
  private OrderByExpressionTreeVisitor<PartitionField>?                                 orderByVisitor_;
  private WhereExpressionTreeVisitor<PartitionField, Filters, FiltersAnd, FilterField>? whereVisitor_;

  public PartitionQueryExpressionTreeVisitor()
  {
    // By default the requests are ordered by PartitionId in ascending order
    SortCriteria = new PartitionField
                   {
                     PartitionRawField = new PartitionRawField
                                         {
                                           Field = PartitionRawEnumField.Id,
                                         },
                   };
    IsSortAscending = true;
  }

  protected override bool IsWhereExpressionTreeVisitorInstantiated
    => whereVisitor_ != null;

  protected override WhereExpressionTreeVisitor<PartitionField, Filters, FiltersAnd, FilterField> WhereExpressionTreeVisitor
  {
    get
    {
      whereVisitor_ = whereVisitor_ ?? new PartitionWhereExpressionTreeVisitor();
      return whereVisitor_;
    }
  }

  protected override OrderByExpressionTreeVisitor<PartitionField> OrderByWhereExpressionTreeVisitor
  {
    get
    {
      orderByVisitor_ = orderByVisitor_ ?? new PartitionOrderByExpressionTreeVisitor();
      return orderByVisitor_;
    }
  }
}
