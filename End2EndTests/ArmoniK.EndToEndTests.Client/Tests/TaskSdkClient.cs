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

using ArmoniK.Extensions.CSharp.Client.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Client.Common.Domain.Task;
using ArmoniK.Extensions.CSharp.Client.Handles;
using ArmoniK.Extensions.CSharp.Client.Queryable;

namespace ArmoniK.EndToEndTests.Client.Tests;

public class TaskSdkClient : ClientBase
{
  [SetUp]
  public async Task SetupAsync()
    => await SetupBaseAsync("TaskSdkWorker")
         .ConfigureAwait(false);

  [TearDown]
  public async Task TearDownAsync()
    => await TearDownBaseAsync()
         .ConfigureAwait(false);

  [Test]
  [TestCase(100)]
  [TestCase(100000)]
  public async Task TaskSdk(int size)
  {
    var str      = $"String of size {size}: {new string('A', size)}";
    var callback = new Callback();
    var taskDefinition = new TaskDefinition().WithLibrary(WorkerLibrary!)
                                             .WithInput("inputString",
                                                        BlobDefinition.FromString("blobInputString",
                                                                                  str))
                                             .WithOutput("outputString",
                                                         BlobDefinition.CreateOutput("blobOutputString")
                                                                       .WithCallback(callback))
                                             .WithTaskOptions(TaskConfiguration!);
    SessionHandle!.Submit([taskDefinition]);

    await SessionHandle.WaitCallbacksAsync()
                       .ConfigureAwait(false);

    var resultString = "";
    var outputName = taskDefinition.Outputs.Single()
                                   .Key;
    var outputBlobHandle = taskDefinition.Outputs.Single()
                                         .Value.BlobHandle;
    var inputBlobHandle = taskDefinition.InputDefinitions.Single()
                                        .Value.BlobHandle;
    if (outputName == "outputString")
    {
      resultString = Encoding.UTF8.GetString(callback.Result);
    }

    Assert.Multiple(() =>
                    {
                      Assert.That(inputBlobHandle!.BlobInfo.BlobName,
                                  Is.EqualTo("blobInputString"));
                      Assert.That(outputBlobHandle!.BlobInfo.BlobName,
                                  Is.EqualTo("blobOutputString"));
                      Assert.That(resultString,
                                  Is.EqualTo(str));
                    });
  }

  [Test]
  public async Task TaskQueries()
  {
    var callback = new Callback();
    var taskDefinition = new TaskDefinition().WithLibrary(WorkerLibrary!)
                                             .WithInput("inputString",
                                                        BlobDefinition.FromString("blobInputString",
                                                                                  "Hello!"))
                                             .WithOutput("outputString",
                                                         BlobDefinition.CreateOutput("blobOutputString")
                                                                       .WithCallback(callback))
                                             .WithTaskOptions(TaskConfiguration!);
    var taskInfo = await Client!.TasksService.SubmitTasksAsync(SessionHandle!,
                                                               [taskDefinition])
                                .FirstAsync()
                                .ConfigureAwait(false);

    var allTasks = await Client!.TasksService.AsQueryable()
                                .Where(task => task.SessionId == SessionHandle!.SessionInfo.SessionId)
                                .ToAsyncEnumerable()
                                .ToArrayAsync()
                                .ConfigureAwait(false);
    var taskSummary1 = Client!.TasksService.AsQueryable()
                              .Where(task => task.TaskId == taskInfo.TaskId)
                              .First();
    var taskSummary2 = Client!.TasksService.AsQueryable()
                              .Where(task => task.TaskId == taskInfo.TaskId)
                              .FirstOrDefault();
    var taskDetailed = await Client!.TasksService.AsQueryable()
                                    .Where(task => task.TaskId == taskInfo.TaskId)
                                    .AsTaskDetailed()
                                    .FirstAsync()
                                    .ConfigureAwait(false);

    Assert.Multiple(() =>
                    {
                      Assert.That(allTasks,
                                  Has.Exactly(1)
                                     .Items);
                      Assert.That(allTasks[0].TaskId,
                                  Is.EqualTo(taskInfo.TaskId));

                      Assert.That(taskSummary1.TaskId,
                                  Is.EqualTo(taskInfo.TaskId));
                      Assert.That(taskSummary2!.TaskId,
                                  Is.EqualTo(taskInfo.TaskId));
                      Assert.That(taskDetailed.TaskId,
                                  Is.EqualTo(taskInfo.TaskId));
                    });
  }

  [Test]
  public async Task PartitionQuery()
  {
    var partition1 = Client!.PartitionsService.AsQueryable()
                            .Where(partition => partition.PartitionId == Partition)
                            .First();
    var partition2 = Client!.PartitionsService.AsQueryable()
                            .Where(partition => partition.PartitionId == Partition)
                            .FirstOrDefault();

    Assert.Multiple(() =>
                    {
                      Assert.That(partition1.PartitionId,
                                  Is.EqualTo(Partition));
                      Assert.That(partition2!.PartitionId,
                                  Is.EqualTo(Partition));
                    });
  }

  [TestCase(100,
            100)]
  public async Task NumerousCallbacks(int N,
                                      int size)
  {
    var taskDefinitions = new List<TaskDefinition>();
    var str             = $"String of size {size}: {new string('A', size)}";
    for (var i = 0; i < N; i++)
    {
      taskDefinitions.Add(new TaskDefinition().WithLibrary(WorkerLibrary!)
                                              .WithInput("inputString",
                                                         BlobDefinition.FromString("blobInputString" + i,
                                                                                   str))
                                              .WithOutput("outputString",
                                                          BlobDefinition.CreateOutput("blobOutputString" + i)
                                                                        .WithCallback(new Callback()))
                                              .WithTaskOptions(TaskConfiguration!));
    }

    SessionHandle!.Submit(taskDefinitions);
    await SessionHandle.WaitCallbacksAsync()
                       .ConfigureAwait(false);

    foreach (var task in taskDefinitions)
    {
      var resultString = Encoding.UTF8.GetString(((Callback)task.Outputs.Single()
                                                                .Value.CallBack!).Result);

      Assert.That(resultString,
                  Is.EqualTo(str));
    }
  }

  private class Callback : ICallback
  {
    public byte[] Result { get; private set; } = [];

    public ValueTask OnSuccessAsync(BlobHandle        blob,
                                    byte[]            rawData,
                                    CancellationToken cancellationToken)
    {
      Result = rawData;
      return ValueTask.CompletedTask;
    }

    public ValueTask OnErrorAsync(BlobHandle        blob,
                                  Exception?        exception,
                                  CancellationToken cancellationToken)
    {
      Assert.Fail(exception?.Message ?? $"blob {blob.BlobInfo.BlobId} aborted");
      return ValueTask.CompletedTask;
    }
  }
}
