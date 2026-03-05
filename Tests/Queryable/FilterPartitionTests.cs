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

using ArmoniK.Extensions.CSharp.Client.Common.Domain.Partition;
using ArmoniK.Extensions.CSharp.Client.Queryable;
using ArmoniK.Extensions.CSharp.Client.Queryable.PartitionQuery;

using NUnit.Framework;

using Tests.Configuration;

namespace Tests.Queryable;

public class FilterPartitionTests : BasePartitionFilterTests
{
  [Test]
  public void FilterById()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("PartitionId",
                                                    "==",
                                                    "partition1")));

    var query = client.PartitionsService.AsQueryable()
                      .Where(partition => partition.PartitionId == "partition1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var partitionProvider = (PartitionQueryProvider)((ArmoniKQueryable<Partition>)query).Provider;
    Assert.That(partitionProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildPartitionPagination(filter)));
  }

  [Test]
  public void FilterByPodMax()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("PodMax",
                                                 "==",
                                                 5)));

    var query = client.PartitionsService.AsQueryable()
                      .Where(partition => partition.PodMax == 5);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var partitionProvider = (PartitionQueryProvider)((ArmoniKQueryable<Partition>)query).Provider;
    Assert.That(partitionProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildPartitionPagination(filter)));
  }

  [Test]
  public void FilterByPodReserved()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("PodReserved",
                                                 "==",
                                                 5)));

    var query = client.PartitionsService.AsQueryable()
                      .Where(partition => partition.PodReserved == 5);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var partitionProvider = (PartitionQueryProvider)((ArmoniKQueryable<Partition>)query).Provider;
    Assert.That(partitionProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildPartitionPagination(filter)));
  }

  [Test]
  public void FilterByPreemptionPercentage()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("PreemptionPercentage",
                                                 "==",
                                                 10)));

    var query = client.PartitionsService.AsQueryable()
                      .Where(partition => partition.PreemptionPercentage == 10);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var partitionProvider = (PartitionQueryProvider)((ArmoniKQueryable<Partition>)query).Provider;
    Assert.That(partitionProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildPartitionPagination(filter)));
  }

  [Test]
  public void FilterByPriority()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("Priority",
                                                 "==",
                                                 1)));

    var query = client.PartitionsService.AsQueryable()
                      .Where(partition => partition.Priority == 1);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var partitionProvider = (PartitionQueryProvider)((ArmoniKQueryable<Partition>)query).Provider;
    Assert.That(partitionProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildPartitionPagination(filter)));
  }

  [Test]
  public void FilterByParentPartitionIds()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterArray("ParentPartitionIds",
                                                   "Contains",
                                                   "partition1")));

    var query = client.PartitionsService.AsQueryable()
                      .Where(partition => partition.ParentPartitionIds.Contains("partition1"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var partitionProvider = (PartitionQueryProvider)((ArmoniKQueryable<Partition>)query).Provider;
    Assert.That(partitionProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildPartitionPagination(filter)));
  }

  [Test]
  public void ContainsOnPartitionIdCollection()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("PartitionId",
                                                    "==",
                                                    "partition1")),
                         BuildAnd(BuildFilterString("PartitionId",
                                                    "==",
                                                    "partition2")),
                         BuildAnd(BuildFilterString("PartitionId",
                                                    "==",
                                                    "partition3")));

    string[] partitionIds = ["partition1", "partition2", "partition3"];
    var query = client.PartitionsService.AsQueryable()
                      .Where(partition => partitionIds.Contains(partition.PartitionId));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var partitionProvider = (PartitionQueryProvider)((ArmoniKQueryable<Partition>)query).Provider;
    Assert.That(partitionProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildPartitionPagination(filter)));
  }

  [Test]
  public void ContainsOnPodMaxCollection()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("PodMax",
                                                 "==",
                                                 3)),
                         BuildAnd(BuildFilterInt("PodMax",
                                                 "==",
                                                 4)),
                         BuildAnd(BuildFilterInt("PodMax",
                                                 "==",
                                                 5)));

    long[] podMax = [3, 4, 5];
    var query = client.PartitionsService.AsQueryable()
                      .Where(partition => podMax.Contains(partition.PodMax));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var partitionProvider = (PartitionQueryProvider)((ArmoniKQueryable<Partition>)query).Provider;
    Assert.That(partitionProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildPartitionPagination(filter)));
  }
}
