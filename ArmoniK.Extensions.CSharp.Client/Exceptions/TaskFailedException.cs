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

using ArmoniK.Extensions.CSharp.Common.Common.Domain.Blob;

namespace ArmoniK.Extensions.CSharp.Client.Exceptions;

/// <summary>
///   Exception raised when tasks have failed, providing details about the blobs that were aborted or
///   completed.
/// </summary>
public class TaskFailedException : ArmoniKSdkException
{
  /// <summary>
  ///   Initializes a new instance of the TaskFailedException class.
  /// </summary>
  internal TaskFailedException(Exception       ex,
                               List<BlobState> abortedBlobs,
                               List<BlobState> completedBlobs)
    : base(ex)
  {
    AbortedBlobs   = abortedBlobs.ToImmutableArray();
    CompletedBlobs = completedBlobs.ToImmutableArray();
  }

  /// <summary>
  ///   Array of aborted blobs's BlobState.
  /// </summary>
  public ImmutableArray<BlobState> AbortedBlobs { get; init; }

  /// <summary>
  ///   Array of completed blobs's BlobState.
  /// </summary>
  public ImmutableArray<BlobState> CompletedBlobs { get; init; }
}
