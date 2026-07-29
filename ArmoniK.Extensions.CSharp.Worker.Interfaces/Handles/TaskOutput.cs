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

using System.Text;

namespace ArmoniK.Extensions.CSharp.Worker.Interfaces.Handles;

/// <summary>
///   A write-only view of a task's output blob.
/// </summary>
public sealed class TaskOutput
{
  private readonly BlobHandle handle_;

  /// <summary>
  ///   Creates a TaskOutput wrapping a BlobHandle.
  /// </summary>
  /// <param name="handle">The underlying blob handle.</param>
  internal TaskOutput(BlobHandle handle)
    => handle_ = handle;

  /// <summary>
  ///   Asynchronously retrieves the blob id.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob id.</returns>
  public ValueTask<string> GetBlobIdAsync()
    => handle_.GetBlobIdAsync();

  /// <summary>
  ///   Set the blob data
  /// </summary>
  /// <param name="data">The blob's data</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public Task SendResultAsync(byte[]            data,
                              CancellationToken cancellationToken = default)
    => handle_.SendResultAsync(data,
                               cancellationToken);

  /// <summary>
  ///   Set the blob data
  /// </summary>
  /// <param name="data">The string result</param>
  /// <param name="encoding">Encoding used for the string, when null UTF-8 is used</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public Task SendStringResultAsync(string            data,
                                    Encoding?         encoding          = null,
                                    CancellationToken cancellationToken = default)
    => handle_.SendStringResultAsync(data,
                                     encoding,
                                     cancellationToken);

  /// <summary>
  ///   Exposes the underlying blob handle, e.g. to delegate this output to a sub-task.
  /// </summary>
  /// <returns>The underlying blob handle.</returns>
  public BlobHandle AsBlobHandle()
    => handle_;
}
