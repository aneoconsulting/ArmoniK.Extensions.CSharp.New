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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Extensions.CSharp.Common.Common.Domain.Blob;

namespace ArmoniK.Extensions.CSharp.Client.Handles;

/// <summary>
///   Provides methods for handling operations related to blobs, such as retrieving state, downloading, and uploading
///   blob data.
/// </summary>
public class BlobHandle
{
  /// <summary>
  ///   The ArmoniK client used for performing blob operations.
  /// </summary>
  public readonly ArmoniKClient ArmoniKClient;

  /// <summary>
  ///   Promise of the BlobInfo once the blob has actually been created.
  ///   It needs to be volatile as we need it to play the role of a barrier in GetBlobInfoAsync().
  /// </summary>
  private volatile TaskCompletionSource<BlobInfo>? blobInfoSource_;

  /// <summary>
  ///   The blob info once it is known.
  /// </summary>
  private BlobInfo? blobInfo_;

  /// <summary>
  ///   Initializes a new instance of the <see cref="BlobHandle" /> class with specified blob information and an ArmoniK
  ///   client. The handle is already resolved.
  /// </summary>
  /// <param name="blobInfo">The information about the blob.</param>
  /// <param name="armoniKClient">The ArmoniK client used for performing blob operations.</param>
  public BlobHandle(BlobInfo      blobInfo,
                    ArmoniKClient armoniKClient)

  {
    blobInfo_     = blobInfo      ?? throw new ArgumentNullException(nameof(blobInfo));
    ArmoniKClient = armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient));
  }

  /// <summary>
  ///   Initializes a new instance of the <see cref="BlobHandle" /> class with specified blob details and an ArmoniK
  ///   client. The handle is already resolved.
  /// </summary>
  /// <param name="blobName">The name of the blob.</param>
  /// <param name="blobId">The identifier of the blob.</param>
  /// <param name="sessionId">The session identifier associated with the blob.</param>
  /// <param name="armoniKClient">The ArmoniK client used for performing blob operations.</param>
  public BlobHandle(string        blobName,
                    string        blobId,
                    string        sessionId,
                    ArmoniKClient armoniKClient)
  {
    blobInfo_ = new BlobInfo
                {
                  BlobId    = blobId,
                  BlobName  = blobName,
                  SessionId = sessionId,
                };
    ArmoniKClient = armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient));
  }

  /// <summary>
  ///   Initializes a new instance of the <see cref="BlobHandle" /> class that is not resolved yet.
  ///   Its <see cref="BlobInfo" /> will be known once <see cref="BlobInfoSource" /> is completed.
  /// </summary>
  /// <param name="armoniKClient">The ArmoniK client used for performing blob operations.</param>
  internal BlobHandle(ArmoniKClient armoniKClient)
  {
    ArmoniKClient   = armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient));
    blobInfoSource_ = new TaskCompletionSource<BlobInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
  }

  /// <summary>
  ///   The TaskCompletionSource valued once the blob has actually been created, null if the handle is already resolved.
  /// </summary>
  internal TaskCompletionSource<BlobInfo>? BlobInfoSource
    => blobInfoSource_;

  /// <summary>
  ///   Whenever the handle is already resolved, returns its BlobInfo, null otherwise.
  /// </summary>
  internal BlobInfo? ResolvedBlobInfoOrNull
    => blobInfo_;

  /// <summary>
  ///   Creates a BlobHandle from BlobInfo and ArmoniKClient.
  /// </summary>
  /// <param name="blobInfo">The BlobInfo to wrap.</param>
  /// <param name="armoniKClient">The ArmoniK client for operations.</param>
  /// <returns>A new BlobHandle instance.</returns>
  /// <exception cref="ArgumentNullException">Thrown when blobInfo or armoniKClient is null.</exception>
  public static BlobHandle FromBlobInfo(BlobInfo      blobInfo,
                                        ArmoniKClient armoniKClient)
    => new(blobInfo      ?? throw new ArgumentNullException(nameof(blobInfo)),
           armoniKClient ?? throw new ArgumentNullException(nameof(armoniKClient)));

  /// <summary>
  ///   Asynchronously retrieves the BlobInfo of the blob, waiting for the blob to be created if necessary.
  /// </summary>
  /// <returns>A task representing the asynchronous operation. The task result contains the BlobInfo instance.</returns>
  public ValueTask<BlobInfo> GetBlobInfoAsync()
  {
    var blobInfo = blobInfo_;
    if (blobInfo is not null)
    {
      return new ValueTask<BlobInfo>(blobInfo);
    }

    return Core();

    async ValueTask<BlobInfo> Core()
    {
      // volatile read of blobInfoSource_ here has acquire semantics and with the combination
      // of the volatile write of blobInfoSource_ below, it ensures that if we see a null value for blobInfoSource_,
      // we are guaranteed to see a non-null value for blobInfo_.
      var tcs = blobInfoSource_;
      if (tcs is null)
      {
        return blobInfo_!;
      }

      var resolvedBlobInfo = await tcs.Task.ConfigureAwait(false);
      blobInfo_ = resolvedBlobInfo;
      // volatile write of blobInfoSource_ here has release semantics (allows other threads to see the effects of preceding operations).
      // This prevent the compiler to do some operation reordering, then we are sure blobInfo_ has actually been assigned when we reach that point
      // therefore if a thread can see a null blobInfoSource_, it is guaranteed to see a non-null blobInfo_.
      blobInfoSource_ = null;
      return resolvedBlobInfo;
    }
  }

  /// <summary>
  ///   Asynchronously retrieves the state of the blob.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob state.</returns>
  public async Task<BlobState> GetBlobStateAsync(CancellationToken cancellationToken = default)
  {
    var blobInfo = await GetBlobInfoAsync()
                     .ConfigureAwait(false);
    return await ArmoniKClient.BlobService.GetBlobStateAsync(blobInfo,
                                                             cancellationToken)
                              .ConfigureAwait(false);
  }

  /// <summary>
  ///   Asynchronously downloads the data of the blob.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation. The task result contains the blob content as a byte array.</returns>
  public async Task<byte[]> DownloadBlobDataAsync(CancellationToken cancellationToken)
  {
    var blobInfo = await GetBlobInfoAsync()
                     .ConfigureAwait(false);
    return await ArmoniKClient.BlobService.DownloadBlobAsync(blobInfo,
                                                             cancellationToken)
                              .ConfigureAwait(false);
  }

  /// <summary>
  ///   Asynchronously downloads the data of the blob in chunks.
  /// </summary>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>An asynchronous enumerable of byte arrays representing the blob data chunks.</returns>
  public async IAsyncEnumerable<byte[]> DownloadBlobDataWithChunksAsync([EnumeratorCancellation] CancellationToken cancellationToken)
  {
    var blobInfo = await GetBlobInfoAsync()
                     .ConfigureAwait(false);
    await foreach (var chunk in ArmoniKClient.BlobService.DownloadBlobWithChunksAsync(blobInfo,
                                                                                      cancellationToken)
                                             .ConfigureAwait(false))
    {
      yield return chunk;
    }
  }

  /// <summary>
  ///   Asynchronously uploads the specified content to the blob.
  /// </summary>
  /// <param name="blobContent">The content to upload to the blob.</param>
  /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
  /// <returns>A task representing the asynchronous operation.</returns>
  public async Task UploadBlobDataAsync(ReadOnlyMemory<byte> blobContent,
                                        CancellationToken    cancellationToken)
  {
    var blobInfo = await GetBlobInfoAsync()
                     .ConfigureAwait(false);
    // Upload the blob chunk
    await ArmoniKClient.BlobService.UploadBlobAsync(blobInfo,
                                                    blobContent,
                                                    cancellationToken)
                       .ConfigureAwait(false);
  }
}
