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

namespace ArmoniK.Extensions.CSharp.Client.Common.Domain.Blob;

/// <summary>
///   Description of a blob to be produced as a task output. Unlike <see cref="InputBlobDefinition" />, it can never
///   wrap an already-existing blob: a task output is always a fresh blob.
/// </summary>
public sealed class OutputBlobDefinition : BlobDefinition
{
  private OutputBlobDefinition(string name)
    : base(name)
  {
  }

  /// <summary>
  ///   Create an output blob definition
  /// </summary>
  /// <param name="name">The blob name</param>
  /// <returns>The newly created blob definition</returns>
  public static OutputBlobDefinition CreateOutput(string name)
    => new(name);

  /// <inheritdoc cref="BlobDefinition.WithManualDeletion" />
  public new OutputBlobDefinition WithManualDeletion()
  {
    base.WithManualDeletion();
    return this;
  }

  /// <inheritdoc cref="BlobDefinition.WithCallback" />
  public new OutputBlobDefinition WithCallback(ICallback callBack)
  {
    base.WithCallback(callBack);
    return this;
  }
}
