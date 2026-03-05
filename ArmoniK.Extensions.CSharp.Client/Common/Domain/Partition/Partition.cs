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
using System.Collections.Immutable;
using System.Linq;

using ArmoniK.Api.gRPC.V1.Partitions;

namespace ArmoniK.Extensions.CSharp.Client.Common.Domain.Partition;

/// <summary>
///   Represents a partition within a ArmoniK, detailing its configuration, resource allocation, and
///   hierarchy.
/// </summary>
public record Partition
{
  /// <summary>
  ///   Identifier of the partition.
  /// </summary>
  public string PartitionId { get; init; } = string.Empty;

  /// <summary>
  ///   Collection of identifiers for parent partitions.
  /// </summary>
  public ImmutableArray<string> ParentPartitionIds { get; init; } = ImmutableArray<string>.Empty;

  /// <summary>
  ///   Configuration settings for pods within the partition, represented as key-value pairs.
  /// </summary>
  public IReadOnlyDictionary<string, string> PodConfiguration { get; init; } = new Dictionary<string, string>();

  /// <summary>
  ///   Maximum number of pods that can be allocated to this partition.
  /// </summary>
  public long PodMax { get; init; }

  /// <summary>
  ///   Number of pods that are reserved for this partition.
  /// </summary>
  public long PodReserved { get; init; }

  /// <summary>
  ///   Percentage of the partition's capacity that is subject to preemption.
  /// </summary>
  public long PreemptionPercentage { get; init; }

  /// <summary>
  ///   Priority of the partition, which may influence scheduling decisions.
  /// </summary>
  public long Priority { get; init; }

  /// <summary>
  ///   Override of the equality method to compare two Partition instances based on their properties.
  /// </summary>
  public virtual bool Equals(Partition? other)
  {
    if (other is null)
    {
      return false;
    }

    if (ReferenceEquals(this,
                        other))
    {
      return true;
    }

    return ParentPartitionIds.SequenceEqual(other.ParentPartitionIds) && PodConfiguration.Count == other.PodConfiguration.Count &&
           PodConfiguration.All(kvp => other.PodConfiguration.TryGetValue(kvp.Key,
                                                                          out var v) && v == kvp.Value) && PartitionId == other.PartitionId && PodMax == other.PodMax &&
           PodReserved == other.PodReserved && PreemptionPercentage == other.PreemptionPercentage && Priority == other.Priority;
  }

  /// <summary>
  ///   Override of the GetHashCode method to generate a hash code based on the properties of the Partition instance.
  /// </summary>
  public override int GetHashCode()
  {
    var tagsHash = ParentPartitionIds.Aggregate(0,
                                                (hash,
                                                 s) => HashCode.Combine(hash,
                                                                        s));
    var dictHash = PodConfiguration.OrderBy(kvp => kvp.Key)
                                   .Aggregate(0,
                                              (hash,
                                               kvp) => HashCode.Combine(hash,
                                                                        kvp.Key,
                                                                        kvp.Value));
    return HashCode.Combine(tagsHash,
                            dictHash,
                            PartitionId,
                            PodMax,
                            PodReserved,
                            PreemptionPercentage,
                            Priority);
  }
}

/// <summary>
///   Class of extensions methods to convert Protobuf instances and Partition instances
/// </summary>
public static class PartitionExt
{
  /// <summary>
  ///   Convert a PartitionRaw instance into a Partition instance
  /// </summary>
  /// <param name="partitionRaw">The PartitionRaw instance</param>
  /// <returns>The Partition instance</returns>
  public static Partition ToPartition(this PartitionRaw partitionRaw)
    => new()
       {
         PartitionId        = partitionRaw.Id,
         ParentPartitionIds = partitionRaw.ParentPartitionIds.ToImmutableArray(),
         PodConfiguration = partitionRaw.PodConfiguration.ToDictionary(pair => pair.Key,
                                                                       pair => pair.Value),
         PodMax               = partitionRaw.PodMax,
         PodReserved          = partitionRaw.PodReserved,
         PreemptionPercentage = partitionRaw.PreemptionPercentage,
         Priority             = partitionRaw.Priority,
       };
}
