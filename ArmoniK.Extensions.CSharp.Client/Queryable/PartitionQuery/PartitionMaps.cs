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

using ArmoniK.Api.gRPC.V1.Partitions;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Partition;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.PartitionQuery;

internal static class PartitionMaps
{
  public static readonly Dictionary<string, PartitionRawEnumField> MemberName2EnumField_ = new()
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

  public static readonly Dictionary<string, Type> MemberName2Type_ = new()
                                                                     {
                                                                       {
                                                                         nameof(Partition.PartitionId), typeof(string)
                                                                       },
                                                                       {
                                                                         nameof(Partition.PodMax), typeof(long)
                                                                       },
                                                                       {
                                                                         nameof(Partition.PodReserved), typeof(long)
                                                                       },
                                                                       {
                                                                         nameof(Partition.PreemptionPercentage), typeof(long)
                                                                       },
                                                                       {
                                                                         nameof(Partition.Priority), typeof(long)
                                                                       },
                                                                       {
                                                                         nameof(Partition.ParentPartitionIds), typeof(IReadOnlyCollection<string>)
                                                                       },
                                                                     };
}
