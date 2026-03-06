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
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Partition;
using ArmoniK.Extensions.CSharp.Client.Common.Enum;
using ArmoniK.Extensions.CSharp.Client.Common.Generic;
using ArmoniK.Extensions.CSharp.Client.Common.Services;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.PartitionQuery;

/// <summary>
///   Specialisation of QueryExecution for queries on Partition instances.
/// </summary>
internal class PartitionQueryExecution : QueryExecution<PartitionPage, Partition, PartitionField, Filters, FiltersAnd, FilterField>
{
  private readonly ILogger<IPartitionsService> logger_;
  private readonly IPartitionsService          partitionService_;

  public PartitionQueryExecution(IPartitionsService          service,
                                 ILogger<IPartitionsService> logger)
  {
    partitionService_ = service;
    logger_           = logger;
  }

  protected override void LogError(Exception ex,
                                   string    message)
    => logger_.LogError(ex,
                        message);

  protected override Pagination<Filters, PartitionField> CreatePaginationInstance(
    QueryExpressionTreeVisitor<Partition, PartitionField, Filters, FiltersAnd, FilterField> visitor)
    => new PartitionPagination
       {
         Filter = visitor.Filters!,
         Page   = 0,
         PageSize = visitor.PageSize.HasValue
                      ? visitor.PageSize.Value
                      : 1000,
         SortDirection = visitor.IsSortAscending
                           ? SortDirection.Asc
                           : SortDirection.Desc,
         SortField = visitor.SortCriteria,
       };

  protected override QueryExpressionTreeVisitor<Partition, PartitionField, Filters, FiltersAnd, FilterField> CreateQueryExpressionTreeVisitor()
    => new PartitionQueryExpressionTreeVisitor();

  protected override Task<PartitionPage> RequestInstancesAsync(Pagination<Filters, PartitionField> pagination,
                                                               CancellationToken                   cancellationToken)
    => partitionService_.ListPartitionsAsync((PartitionPagination)pagination,
                                             cancellationToken);

  protected override object[] GetPageElements(Pagination<Filters, PartitionField> pagination,
                                              PartitionPage                       page)
    => page.Partitions;

  protected override int GetTotalPageElements(PartitionPage page)
    => page.TotalPartitionCount;
}
