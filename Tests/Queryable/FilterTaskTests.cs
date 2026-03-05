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
using ArmoniK.Extensions.CSharp.Client.Queryable;
using ArmoniK.Extensions.CSharp.Client.Queryable.TaskSummaryQuery;

using NUnit.Framework;

using Tests.Configuration;

using TaskStatus = ArmoniK.Extensions.CSharp.Common.Common.Domain.Task.TaskStatus;
using TaskSummary = ArmoniK.Extensions.CSharp.Client.Common.Domain.Task.TaskSummary;

namespace Tests.Queryable;

public class FilterTaskTests : BaseTaskFilterTests
{
  [Test]
  public void TaskIdFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskId == "task1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void DetailedTaskIdFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskId == "task1");

    // Execute the query
    var result = query.AsTaskDetailed()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter,
                                               useDetailedVersion: true)));
  }

  [Test]
  public void SessionIdFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.SessionId == "session1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void PayloadIdFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("PayloadId",
                                                    "==",
                                                    "payload1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.PayloadId == "payload1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void CreateAtFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.UtcNow;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("CreateAt",
                                                      "==",
                                                      date)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.CreateAt == date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void StartedAtFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.UtcNow;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("StartedAt",
                                                      "==",
                                                      date)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.StartedAt == date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void EndedAtFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var date = DateTime.UtcNow;
    var filter = BuildOr(BuildAnd(BuildFilterDateTime("EndedAt",
                                                      "==",
                                                      date)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.EndedAt == date);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void StatusFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterStatus("Status",
                                                    "==",
                                                    TaskStatus.Error)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.Status == TaskStatus.Error);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void MaxDurationFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var duration = new TimeSpan(1000);
    var filter = BuildOr(BuildAnd(BuildFilterDuration("MaxDuration",
                                                      "==",
                                                      duration)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.MaxDuration == duration);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void MaxRetriesFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("MaxRetries",
                                                 "==",
                                                 2)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.MaxRetries == 2);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void PriorityFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("Priority",
                                                 "==",
                                                 1)));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Priority == 1);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void PartitionIdFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("PartitionId",
                                                    "==",
                                                    "partition1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.PartitionId == "partition1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OptionsFilterEqual()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "==",
                                                    "lib1")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Options["library"] == "lib1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OptionsFilterEqualWithLambda()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "==",
                                                    "lib1")));

    var foo = () => "library";
    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Options[foo()] == "lib1");

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }


  [Test]
  public void OptionsFilterStartsWith()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "StartsWith",
                                                    "lib")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Options["library"]
                                                   .StartsWith("lib"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OptionsFilterEndsWith()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "EndsWith",
                                                    "lib")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Options["library"]
                                                   .EndsWith("lib"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OptionsFilterContains()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "Contains",
                                                    "lib")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskOptions.Options["library"]
                                                   .Contains("lib"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OptionsFilterNotContains()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:library",
                                                    "NotContains",
                                                    "lib")));

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => !taskState.TaskOptions.Options["library"]
                                                    .Contains("lib"));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void OrderByPayloadIdAscending()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1")));
    var sortCriteria = new TaskField
                       {
                         TaskSummaryField = new TaskSummaryField
                                            {
                                              Field = TaskSummaryEnumField.PayloadId,
                                            },
                       };

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskId == "task1")
                      .OrderBy(taskState => taskState.PayloadId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter,
                                               sortCriteria)));
  }

  [Test]
  public void OrderByPartitionIdAscending()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1")));
    var sortCriteria = new TaskField
                       {
                         TaskOptionField = new TaskOptionField
                                           {
                                             Field = TaskOptionEnumField.PartitionId,
                                           },
                       };

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskId == "task1")
                      .OrderBy(taskState => taskState.TaskOptions.PartitionId);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter,
                                               sortCriteria)));
  }

  [Test]
  public void OrderBySpecificOptionAscending()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1")));
    var sortCriteria = new TaskField
                       {
                         TaskOptionGenericField = new TaskOptionGenericField
                                                  {
                                                    Field = "Library",
                                                  },
                       };

    var query = client.TasksService.AsQueryable()
                      .Where(taskState => taskState.TaskId == "task1")
                      .OrderBy(taskState => taskState.TaskOptions.Options["Library"]);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter,
                                               sortCriteria)));
  }

  [Test]
  public void ContainsOnTaskIdCollection()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1")),
                         BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task2")),
                         BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task3")));

    string[] taskIds = ["task1", "task2", "task3"];
    var query = client.TasksService.AsQueryable()
                      .Where(task => taskIds.Contains(task.TaskId));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void ContainsOnSessionIdCollection()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session1")),
                         BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session2")),
                         BuildAnd(BuildFilterString("SessionId",
                                                    "==",
                                                    "session3")));

    string[] sessionIds = ["session1", "session2", "session3"];
    var query = client.TasksService.AsQueryable()
                      .Where(task => sessionIds.Contains(task.SessionId));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void ContainsOnPayloadIdCollection()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("PayloadId",
                                                    "==",
                                                    "payload1")),
                         BuildAnd(BuildFilterString("PayloadId",
                                                    "==",
                                                    "payload2")),
                         BuildAnd(BuildFilterString("PayloadId",
                                                    "==",
                                                    "payload3")));

    string[] payloadIds = ["payload1", "payload2", "payload3"];
    var query = client.TasksService.AsQueryable()
                      .Where(task => payloadIds.Contains(task.PayloadId));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void ContainsOnMaxRetriesCollection()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterInt("MaxRetries",
                                                 "==",
                                                 2)),
                         BuildAnd(BuildFilterInt("MaxRetries",
                                                 "==",
                                                 3)),
                         BuildAnd(BuildFilterInt("MaxRetries",
                                                 "==",
                                                 4)));

    int[] maxRetries = [2, 3, 4];
    var query = client.TasksService.AsQueryable()
                      .Where(task => maxRetries.Contains(task.TaskOptions.MaxRetries));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void ContainsOnGenericFieldCollection()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("Options:key",
                                                    "==",
                                                    "key1")),
                         BuildAnd(BuildFilterString("Options:key",
                                                    "==",
                                                    "key2")),
                         BuildAnd(BuildFilterString("Options:key",
                                                    "==",
                                                    "key3")));

    string[] keys = ["key1", "key2", "key3"];
    var query = client.TasksService.AsQueryable()
                      .Where(task => keys.Contains(task.TaskOptions.Options["key"]));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public async Task ContainsOnTaskIdEmptyCollectionAndStatusFilter()
  {
    var client = new MockedArmoniKClient();

    string[] taskIds = [];
    var query = client.TasksService.AsQueryable()
                      .Where(task => taskIds.Contains(task.TaskId) && task.Status == TaskStatus.Submitted);

    // Execute the query
    var result = await query.AsAsyncEnumerable()
                            .ToListAsync()
                            .ConfigureAwait(false);

    Assert.That(result,
                Is.Empty);
  }

  [Test]
  public void NotContainsOnTaskIdEmptyCollectionAndStatusFilter()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterStatus("Status",
                                                    "==",
                                                    TaskStatus.Submitted)));

    string[] taskIds = [];
    var query = client.TasksService.AsQueryable()
                      .Where(task => !taskIds.Contains(task.TaskId) && task.Status == TaskStatus.Submitted);

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }

  [Test]
  public void ContainsOnTaskIdCollectionAndStatusFilter()
  {
    var client = new MockedArmoniKClient();

    var filter = BuildOr(BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1"),
                                  BuildFilterStatus("Status",
                                                    "==",
                                                    TaskStatus.Submitted)),
                         BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task1"),
                                  BuildFilterStatus("Status",
                                                    "==",
                                                    TaskStatus.Completed)),
                         BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task2"),
                                  BuildFilterStatus("Status",
                                                    "==",
                                                    TaskStatus.Submitted)),
                         BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task2"),
                                  BuildFilterStatus("Status",
                                                    "==",
                                                    TaskStatus.Completed)),
                         BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task3"),
                                  BuildFilterStatus("Status",
                                                    "==",
                                                    TaskStatus.Submitted)),
                         BuildAnd(BuildFilterString("TaskId",
                                                    "==",
                                                    "task3"),
                                  BuildFilterStatus("Status",
                                                    "==",
                                                    TaskStatus.Completed)));

    string[] taskIds = ["task1", "task2", "task3"];
    var query = client.TasksService.AsQueryable()
                      .Where(task => taskIds.Contains(task.TaskId) && (task.Status == TaskStatus.Submitted || task.Status == TaskStatus.Completed));

    // Execute the query
    var result = query.AsAsyncEnumerable()
                      .ToListAsync();

    var taskQueryProvider = (TaskSummaryQueryProvider)((ArmoniKQueryable<TaskSummary>)query).Provider;
    Assert.That(taskQueryProvider.QueryExecution!.PaginationInstance,
                Is.EqualTo(BuildTaskPagination(filter)));
  }
}
