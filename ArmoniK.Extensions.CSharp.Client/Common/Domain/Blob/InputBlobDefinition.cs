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
using System.IO;
using System.Text;

using ArmoniK.Extensions.CSharp.Client.Exceptions;
using ArmoniK.Extensions.CSharp.Client.Handles;

namespace ArmoniK.Extensions.CSharp.Client.Common.Domain.Blob;

/// <summary>
///   Description of a blob to be used as a task input.
/// </summary>
public sealed class InputBlobDefinition : BlobDefinition
{
  private InputBlobDefinition(string               name,
                              ReadOnlyMemory<byte> content)
    : base(name,
           content)
  {
  }

  private InputBlobDefinition(string   name,
                              FileInfo file)
    : base(name,
           file)
  {
  }

  private InputBlobDefinition(string name)
    : base(name)
  {
  }

  /// <summary>
  ///   Creates an InputBlobDefinition from a blob handle. The handle must already be resolved (i.e. reference an
  ///   existing blob), since the blob's name is required immediately.
  /// </summary>
  /// <param name="handle">The blob handle</param>
  /// <returns>The newly created blob definition</returns>
  /// <exception cref="ArmoniKSdkException">Thrown when the handle is not resolved yet.</exception>
  public static InputBlobDefinition FromBlobHandle(BlobHandle handle)
  {
    var blobInfo = handle.ResolvedBlobInfoOrNull ?? throw new ArmoniKSdkException("The blob handle must be resolved before it can be used to create a BlobDefinition.");
    return new InputBlobDefinition(blobInfo.BlobName)
           {
             blobHandle_ = handle,
           };
  }

  /// <summary>
  ///   Creates an InputBlobDefinition from a file
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="filePath">The file containing the data</param>
  /// <returns>The newly created blob definition</returns>
  public static InputBlobDefinition FromFile(string blobName,
                                             string filePath)
  {
    var file = new FileInfo(filePath);
    if (!file.Exists)
    {
      throw new ArmoniKSdkException($"The file {file.FullName} does not exists.");
    }

    return new InputBlobDefinition(blobName,
                                   file);
  }

  /// <summary>
  ///   Creates an InputBlobDefinition from a string
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="content">The raw data</param>
  /// <param name="encoding">The encoding used for the string, when null UTF-8 is used</param>
  /// <returns>The newly created blob definition</returns>
  public static InputBlobDefinition FromString(string    blobName,
                                               string    content,
                                               Encoding? encoding = null)
    => new(blobName,
           (encoding ?? Encoding.UTF8).GetBytes(content)
                                      .AsMemory());

  /// <summary>
  ///   Creates an InputBlobDefinition from a read only memory
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="content">The raw data</param>
  /// <returns>The newly created blob definition</returns>
  public static InputBlobDefinition FromReadOnlyMemory(string               blobName,
                                                       ReadOnlyMemory<byte> content)
    => new(blobName,
           content);

  /// <summary>
  ///   Creates an InputBlobDefinition from a byte array
  /// </summary>
  /// <param name="blobName">The blob name</param>
  /// <param name="content">The raw data</param>
  /// <returns>The newly created blob definition</returns>
  public static InputBlobDefinition FromByteArray(string blobName,
                                                  byte[] content)
    => new(blobName,
           content);

  /// <inheritdoc cref="BlobDefinition.WithManualDeletion" />
  public new InputBlobDefinition WithManualDeletion()
  {
    base.WithManualDeletion();
    return this;
  }

  /// <inheritdoc cref="BlobDefinition.WithCallback" />
  public new InputBlobDefinition WithCallback(ICallback callBack)
  {
    base.WithCallback(callBack);
    return this;
  }
}
