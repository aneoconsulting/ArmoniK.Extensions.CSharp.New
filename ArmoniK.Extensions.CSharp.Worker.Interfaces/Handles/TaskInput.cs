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
///   A read-only view of a task's input blob.
/// </summary>
public sealed class TaskInput
{
  private readonly BlobHandle handle_;

  /// <summary>
  ///   Creates a TaskInput wrapping a resolved BlobHandle.
  /// </summary>
  /// <param name="handle">The underlying blob handle.</param>
  internal TaskInput(BlobHandle handle)
    => handle_ = handle;

  /// <summary>
  ///   The input's raw data.
  /// </summary>
  public byte[]? RawData
    => handle_.Data;

  /// <summary>
  ///   Asynchronously retrieves the blob id.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob id.</returns>
  public ValueTask<string> GetBlobIdAsync()
    => handle_.GetBlobIdAsync();

  /// <summary>
  ///   Decodes the input's data as a string with a given encoding.
  /// </summary>
  /// <param name="encoding">Encoding used for the string, when null UTF-8 is used</param>
  /// <returns>The resulting string</returns>
  public string GetStringData(Encoding? encoding = null)
    => handle_.GetStringData(encoding);

  /// <summary>
  ///   Exposes the underlying blob handle, e.g. to reuse this input as another task's input.
  /// </summary>
  /// <returns>The underlying blob handle.</returns>
  public BlobHandle AsBlobHandle()
    => handle_;

  /// <summary>
  ///   Implicitly exposes the underlying blob handle, e.g. to reuse this input as another task's input.
  /// </summary>
  /// <param name="input">The TaskInput to convert.</param>
  public static implicit operator BlobHandle(TaskInput input)
    => input.handle_;
}
