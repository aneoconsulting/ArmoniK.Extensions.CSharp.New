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

using ArmoniK.Api.gRPC.V1.Tasks;
using ArmoniK.Extensions.CSharp.Client;
using ArmoniK.Extensions.CSharp.Client.Handles;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;

using NUnit.Framework;

using Tests.Configuration;

namespace Tests.Handles;

[TestFixture]
public class TaskHandleTests
{
  [SetUp]
  public void SetUp()
  {
    mockedArmoniKClient_ = new MockedArmoniKClient();

    var grpcTaskInfo = new SubmitTasksResponse.Types.TaskInfo
                       {
                         TaskId    = "testTaskId",
                         PayloadId = "testPayloadId",
                         ExpectedOutputIds =
                         {
                           "output1",
                           "output2",
                         },
                         DataDependencies =
                         {
                           "dep1",
                           "dep2",
                         },
                       };

    mockTaskInfos_ = grpcTaskInfo.ToTaskInfos("testSessionId");
  }

  private MockedArmoniKClient? mockedArmoniKClient_;
  private TaskInfos?           mockTaskInfos_;

  [Test]
  public async Task ConstructorShouldInitializeProperties()
  {
    var taskHandle = TaskHandle.FromTaskInfos(mockTaskInfos_!, mockedArmoniKClient_!);
    var convertedTaskInfos = await taskHandle.GetTaskInfosAsync().ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(taskHandle.ArmoniKClient,
                                  Is.Not.Null);
                      Assert.That(taskHandle.ArmoniKClient,
                                  Is.InstanceOf<ArmoniKClient>());

                      Assert.That(convertedTaskInfos,
                                  Is.EqualTo(mockTaskInfos_));
                    });
  }

  [Test]
  public void ConstructorThrowsArgumentNullExceptionWhenClientIsNull()
    => Assert.That(() => TaskHandle.FromTaskInfos(mockTaskInfos_!, null!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("armoniKClient"));

  [Test]
  public void ConstructorThrowsArgumentNullExceptionWhenTaskInfosIsNull()
    => Assert.That(() => TaskHandle.FromTaskInfos(null!, mockedArmoniKClient_!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("taskInfos"));

  [Test]
  public async Task ImplicitConversionToTaskInfosShouldReturnCorrectTaskInfos()
  {
    var taskHandle = TaskHandle.FromTaskInfos(mockTaskInfos_!, mockedArmoniKClient_!);

    TaskInfos convertedTaskInfos = await taskHandle.GetTaskInfosAsync().ConfigureAwait(false);

    Assert.That(convertedTaskInfos,
                Is.EqualTo(mockTaskInfos_));
  }

  [Test]
  public async Task FromTaskInfosCreatesTaskHandleCorrectly()
  {
    var taskHandle = TaskHandle.FromTaskInfos(mockTaskInfos_!,
                                              mockedArmoniKClient_!);
    var convertedTaskInfos = await taskHandle.GetTaskInfosAsync().ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(taskHandle.ArmoniKClient,
                                  Is.Not.Null);
                      Assert.That(taskHandle.ArmoniKClient,
                                  Is.InstanceOf<ArmoniKClient>());

                      Assert.That(convertedTaskInfos,
                                  Is.EqualTo(mockTaskInfos_));
                    });
  }

  [Test]
  public void FromTaskInfosThrowsArgumentNullExceptionWhenTaskInfosIsNull()
    => Assert.That(() => TaskHandle.FromTaskInfos(null!,
                                                  mockedArmoniKClient_!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("taskInfos"));

  [Test]
  public void FromTaskInfosThrowsArgumentNullExceptionWhenClientIsNull()
    => Assert.That(() => TaskHandle.FromTaskInfos(mockTaskInfos_!,
                                                  null!),
                   Throws.ArgumentNullException.With.Property(nameof(ArgumentNullException.ParamName))
                         .EqualTo("armoniKClient"));

  [Test]
  public void ImplicitConversionWorksInMethodParameters()
  {
    var taskHandle = TaskHandle.FromTaskInfos(mockTaskInfos_!, mockedArmoniKClient_!);

    Assert.That(() => Assert.That(taskHandle,
                                  Is.Not.Null),
                Throws.Nothing);
  }

  [Test]
  public void GetTaskDetailsAsyncWithCancellationTokenCancels()
  {
    var taskHandle = TaskHandle.FromTaskInfos(mockTaskInfos_!, mockedArmoniKClient_!);

    var cancellationTokenSource = new CancellationTokenSource();
    cancellationTokenSource.Cancel();

    Assert.That(async () => await taskHandle.GetTaskDetailsAsync(cancellationTokenSource.Token),
                Throws.InstanceOf<OperationCanceledException>());
  }
}
