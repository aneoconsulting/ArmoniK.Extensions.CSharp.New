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
///   A handle allowing to perform some operations on a blob
/// </summary>
public class BlobHandle
{
  private readonly ISdkTaskHandler sdkTaskHandler_;

  /// <summary>
  ///   Promise of the blob id once the blob has actually been created.
  ///   It needs to be volatile as we need it to play the role of a barrier in GetBlobIdAsync().
  /// </summary>
  private volatile TaskCompletionSource<string>? blobIdSource_;

  /// <summary>
  ///   The blob id once it is known.
  /// </summary>
  private string? blobId_;

  /// <summary>
  ///   Creates an of a BlobHandle that is already resolved.
  /// </summary>
  /// <param name="blobId">The blob id</param>
  /// <param name="sdkTaskHandler">The SDK task handler</param>
  /// <param name="data">The blob's raw data for input blobs</param>
  public BlobHandle(string          blobId,
                    ISdkTaskHandler sdkTaskHandler,
                    byte[]?         data = null)
  {
    blobId_         = blobId;
    sdkTaskHandler_ = sdkTaskHandler;
    Data            = data;
  }

  /// <summary>
  ///   Creates a BlobHandle that is not resolved yet. Its blob id will be known once
  ///   <see cref="BlobIdSource" /> is completed.
  /// </summary>
  /// <param name="sdkTaskHandler">The SDK task handler</param>
  internal BlobHandle(ISdkTaskHandler sdkTaskHandler)
  {
    sdkTaskHandler_ = sdkTaskHandler;
    blobIdSource_   = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
  }

  /// <summary>
  ///   The blob raw data, null for output blobs
  /// </summary>
  public byte[]? Data { get; init; }

  /// <summary>
  ///   The TaskCompletionSource valued once the blob has actually been created, null if the handle is already resolved.
  /// </summary>
  internal TaskCompletionSource<string>? BlobIdSource
    => blobIdSource_;

  /// <summary>
  ///   Whenever the handle is already resolved, returns its blob id, null otherwise.
  /// </summary>
  internal string? ResolvedBlobIdOrNull
    => blobId_;

  /// <summary>
  ///   Asynchronously retrieves the blob id, waiting for the blob to be created if necessary.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob id.</returns>
  public ValueTask<string> GetBlobIdAsync()
  {
    var blobId = blobId_;
    if (blobId is not null)
    {
      return new ValueTask<string>(blobId);
    }

    return Core();

    async ValueTask<string> Core()
    {
      // volatile read of blobIdSource_ here has acquire semantics and with the combination
      // of the volatile write of blobIdSource_ below, it ensures that if we see a null value for blobIdSource_,
      // we are guaranteed to see a non-null value for blobId_.
      var tcs = blobIdSource_;
      if (tcs is null)
      {
        return blobId_!;
      }

      var resolvedBlobId = await tcs.Task.ConfigureAwait(false);
      blobId_ = resolvedBlobId;
      // volatile write of blobIdSource_ here has release semantics (allows other threads to see the effects of preceding operations).
      // This prevent the compiler to do some operation reordering, then we are sure blobId_ has actually been assigned when we reach that point
      // therefore if a thread can see a null blobIdSource_, it is guaranteed to see a non-null blobId_.
      blobIdSource_ = null;
      return resolvedBlobId;
    }
  }

  /// <summary>
  ///   Decodes the blob's data as a string with a given encoding.
  /// </summary>
  /// <param name="encoding">Encoding used for the string, when null UTF-8 is used</param>
  /// <returns>The resulting string</returns>
  public string GetStringData(Encoding? encoding = null)
    => (encoding ?? Encoding.UTF8).GetString(Data!);

  /// <summary>
  ///   Set the blob data
  /// </summary>
  /// <param name="data">The blob's data</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task SendResultAsync(byte[]            data,
                                    CancellationToken cancellationToken = default)
    => await sdkTaskHandler_.SendResultAsync(this,
                                             data,
                                             cancellationToken)
                            .ConfigureAwait(false);

  /// <summary>
  ///   Set the blob data
  /// </summary>
  /// <param name="data">The string result</param>
  /// <param name="encoding">Encoding used for the string, when null UTF-8 is used</param>
  /// <param name="cancellationToken">Token used to cancel the execution of the method.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task SendStringResultAsync(string            data,
                                          Encoding?         encoding          = null,
                                          CancellationToken cancellationToken = default)
    => await sdkTaskHandler_.SendStringResultAsync(this,
                                                   data,
                                                   encoding,
                                                   cancellationToken)
                            .ConfigureAwait(false);
}
