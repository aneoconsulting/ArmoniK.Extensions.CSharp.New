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

using ArmoniK.Extensions.CSharp.Client;
using ArmoniK.Extensions.CSharp.Client.Handles;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Blob;

using NUnit.Framework;

using Tests.Configuration;

namespace Tests.Handles;

[TestFixture]
public class BlobHandleTests
{
  [SetUp]
  public void SetUp()
  {
    mockedArmoniKClient_ = new MockedArmoniKClient();
    mockBlobInfo_ = new BlobInfo
                    {
                      BlobName  = "testBlob",
                      SessionId = "testSession",
                      BlobId    = "testBlobId",
                    };
  }

  private MockedArmoniKClient? mockedArmoniKClient_;
  private BlobInfo?            mockBlobInfo_;

  [Test]
  public async Task ConstructorWithBlobInfosShouldInitializeProperties()
  {
    var blobHandle = new BlobHandle(mockBlobInfo_!,
                                    mockedArmoniKClient_!);

    var blobInfo = await blobHandle.GetBlobInfoAsync();

    Assert.Multiple(() =>
                    {
                      Assert.That(blobInfo,
                                  Is.EqualTo(mockBlobInfo_));
                      Assert.That(blobHandle.ArmoniKClient,
                                  Is.Not.Null);
                      Assert.That(blobHandle.ArmoniKClient,
                                  Is.InstanceOf<ArmoniKClient>());
                    });
  }

  [Test]
  public async Task ConstructorWithIndividualParametersShouldInitializeCorrectly()
  {
    var blobName  = "myBlob";
    var blobId    = "myId";
    var sessionId = "mySession";

    var blobHandle = new BlobHandle(blobName,
                                    blobId,
                                    sessionId,
                                    mockedArmoniKClient_!);

    var blobInfo = await blobHandle.GetBlobInfoAsync();

    Assert.Multiple(() =>
                    {
                      Assert.That(blobInfo.BlobName,
                                  Is.EqualTo(blobName));
                      Assert.That(blobInfo.BlobId,
                                  Is.EqualTo(blobId));
                      Assert.That(blobInfo.SessionId,
                                  Is.EqualTo(sessionId));
                      Assert.That(blobHandle.ArmoniKClient,
                                  Is.Not.Null);
                    });
  }

  [Test]
  public void ConstructorWithBlobInfoThrowsArgumentNullExceptionWhenBlobInfoIsNull()
    => Assert.That(() => new BlobHandle(null!,
                                        mockedArmoniKClient_!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("blobInfo"));

  [Test]
  public void ConstructorWithBlobInfo_ThrowsArgumentNullException_WhenClientIsNull()
    => Assert.That(() => new BlobHandle(mockBlobInfo_!,
                                        null!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("armoniKClient"));

  [Test]
  public void ResolvedHandleGetBlobInfoAsyncCompletesSynchronously()
  {
    var blobHandle = new BlobHandle(mockBlobInfo_!,
                                    mockedArmoniKClient_!);

    var valueTask = blobHandle.GetBlobInfoAsync();

    Assert.Multiple(() =>
                    {
                      Assert.That(valueTask.IsCompletedSuccessfully,
                                  Is.True);
                      Assert.That(valueTask.Result,
                                  Is.EqualTo(mockBlobInfo_));
                    });
  }

  [Test]
  public void PendingHandleGetBlobInfoAsyncDoesNotCompleteBeforeResolution()
  {
    var blobHandle = new BlobHandle(mockedArmoniKClient_!);

    var task = blobHandle.GetBlobInfoAsync()
                         .AsTask();

    Assert.That(task.IsCompleted,
                Is.False);
  }

  [Test]
  public async Task PendingHandleGetBlobInfoAsyncCompletesOnceResolved()
  {
    var blobHandle = new BlobHandle(mockedArmoniKClient_!);

    var task = blobHandle.GetBlobInfoAsync()
                         .AsTask();

    blobHandle.BlobInfoSource!.SetResult(mockBlobInfo_!);

    var blobInfo = await task;

    Assert.That(blobInfo,
                Is.EqualTo(mockBlobInfo_));
  }

  [Test]
  public void PendingHandleGetBlobInfoAsyncFaultsWhenFailed()
  {
    var blobHandle = new BlobHandle(mockedArmoniKClient_!);

    var task = blobHandle.GetBlobInfoAsync()
                         .AsTask();

    var expectedException = new InvalidOperationException("blob creation failed");
    blobHandle.BlobInfoSource!.SetException(expectedException);

    Assert.That(async () => await task,
                Throws.InvalidOperationException.With.Message.EqualTo(expectedException.Message));
  }

  [Test]
  public void PendingHandleResolvedBlobInfoOrNullIsNullBeforeResolution()
  {
    var blobHandle = new BlobHandle(mockedArmoniKClient_!);

    Assert.That(blobHandle.ResolvedBlobInfoOrNull,
                Is.Null);
  }
}
