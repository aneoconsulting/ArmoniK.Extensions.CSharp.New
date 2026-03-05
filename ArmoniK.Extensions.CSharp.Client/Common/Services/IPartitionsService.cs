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

using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extensions.CSharp.Client.Common.Domain.Partition;

namespace ArmoniK.Extensions.CSharp.Client.Common.Services;

/// <summary>
///   Defines a service for managing partitions, including retrieval and listing of partitions.
/// </summary>
public interface IPartitionsService
{
  /// <summary>
  ///   Get a queryable object on Partition instances
  /// </summary>
  /// <returns>An IQueryable instance to apply Linq extensions methods on</returns>
  IQueryable<Partition> AsQueryable();

  /// <summary>
  ///   Asynchronously retrieves a partition by its identifier.
  /// </summary>
  /// <param name="partitionId">The identifier of the partition to retrieve.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the partition information.</returns>
  Task<Partition> GetPartitionAsync(string            partitionId,
                                    CancellationToken cancellationToken);

  /// <summary>
  ///   Asynchronously lists partitions based on pagination options.
  /// </summary>
  /// <param name="partitionPagination">The options for pagination, including page number, page size, and sorting.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains a partition page.</returns>
  Task<PartitionPage> ListPartitionsAsync(PartitionPagination partitionPagination,
                                          CancellationToken   cancellationToken);
}
