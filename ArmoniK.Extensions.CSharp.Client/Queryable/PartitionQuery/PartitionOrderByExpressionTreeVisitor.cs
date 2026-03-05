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

using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Partition;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.PartitionQuery;

/// <summary>
///   Specialisation of OrderByExpressionTreeVisitor for queries on Partition instances.
/// </summary>
internal class PartitionOrderByExpressionTreeVisitor : OrderByExpressionTreeVisitor<PartitionField>
{
  public override PartitionField Visit(LambdaExpression lambda)
  {
    if (lambda.Body is MemberExpression member)
    {
      if (PartitionMaps.MemberName2EnumField_.TryGetValue(member.Member.Name,
                                            out var field))
      {
        return new PartitionField
               {
                 PartitionRawField = new PartitionRawField
                                     {
                                       Field = field,
                                     },
               };
      }
    }

    throw new InvalidExpressionException("Invalid partition ordering expression: a sortable Partition property was expected." + Environment.NewLine +
                                         "Expression was: "                                                                   + lambda.Body);
  }
}
