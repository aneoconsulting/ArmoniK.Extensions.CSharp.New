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

using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using ArmoniK.Api.Client;
using ArmoniK.Api.Common.Exceptions;
using ArmoniK.Api.gRPC.V1.Events;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Session;
using ArmoniK.Extensions.CSharp.Client.Common.Services;
using ArmoniK.Extensions.CSharp.Client.Exceptions;
using ArmoniK.Extensions.CSharp.Client.Queryable;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Blob;
using ArmoniK.Utils.Pool;

using Grpc.Core;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extensions.CSharp.Client.Services;

/// <inheritdoc />
public class EventsService : IEventsService
{
  private readonly ObjectPool<ChannelBase> channel_;
  private readonly ArmoniKClient           client_;
  private readonly ILogger<EventsService>  logger_;

  /// <summary>
  ///   Creates an instance of <see cref="EventsService" /> using the specified GRPC channel and an optional logger
  ///   factory.
  /// </summary>
  /// <param name="channel">
  ///   An object pool that manages GRPC channels, providing efficient handling and reuse of channel
  ///   resources.
  /// </param>
  /// <param name="client">The ArmoniK client instance</param>
  /// <param name="loggerFactory">
  ///   An optional factory for creating loggers, which can be used to enable logging within the
  ///   events service. If null, logging will be disabled.
  /// </param>
  public EventsService(ObjectPool<ChannelBase> channel,
                       ArmoniKClient           client,
                       ILoggerFactory          loggerFactory)
  {
    channel_ = channel;
    client_  = client;
    logger_  = loggerFactory.CreateLogger<EventsService>();
  }

  /// <inheritdoc />
  public async Task WaitForBlobsAsync(SessionInfo           session,
                                      ICollection<BlobInfo> blobInfos,
                                      CancellationToken     cancellationToken = default)
  {
    await using var channel = await channel_.GetAsync(cancellationToken)
                                            .ConfigureAwait(false);
    var eventsClient = new Events.EventsClient(channel);
    try
    {
      await eventsClient.WaitForResultsAsync(session.SessionId,
                                             blobInfos.Select(x => x.BlobId)
                                                      .ToList(),
                                             100,
                                             1,
                                             cancellationToken)
                        .ConfigureAwait(false);
    }
    catch (ResultAbortedException ex)
    {
      logger_.LogError(ex,
                       "Waiting for blobs was aborted for session {SessionId}",
                       session.SessionId);

      var abortedBlobs   = new List<BlobState>();
      var completedBlobs = new List<BlobState>();
      await foreach (var blob in client_.BlobService.AsQueryable()
                                        .Where(blob => blobInfos.Select(blobInfo => blobInfo.BlobId)
                                                                .Contains(blob.BlobId) && (blob.Status == BlobStatus.Aborted || blob.Status == BlobStatus.Completed))
                                        .WithPageSize(500)
                                        .ToAsyncEnumerable()
                                        .ConfigureAwait(false))
      {
        if (blob.Status == BlobStatus.Aborted)
        {
          logger_.LogError("In session '{SessionId}' blob '{BlobId}' was Aborted. It's task owner is '{TaskId}'",
                           session.SessionId,
                           blob.BlobId,
                           blob.OwnerId);
          abortedBlobs.Add(blob);
        }
        else
        {
          completedBlobs.Add(blob);
        }
      }

      throw new TaskFailedException(ex,
                                    abortedBlobs,
                                    completedBlobs);
    }
  }
}
