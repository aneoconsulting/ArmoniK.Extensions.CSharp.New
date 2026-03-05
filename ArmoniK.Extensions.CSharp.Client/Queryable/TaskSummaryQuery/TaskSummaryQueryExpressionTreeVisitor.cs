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

using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1.Tasks;

using TaskSummary = ArmoniK.Extensions.CSharp.Client.Common.Domain.Task.TaskSummary;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.TaskSummaryQuery;

/// <summary>
///   Specialisation of QueryExpressionTreeVisitor for queries on TaskState instances.
/// </summary>
internal class TaskSummaryQueryExpressionTreeVisitor : QueryExpressionTreeVisitor<TaskSummary, TaskField, Filters, FiltersAnd, FilterField>
{
  private OrderByExpressionTreeVisitor<TaskField>?                                 orderByVisitor_;
  private WhereExpressionTreeVisitor<TaskField, Filters, FiltersAnd, FilterField>? whereVisitor_;

  public TaskSummaryQueryExpressionTreeVisitor()
  {
    // By default the requests are ordered by TaskId in ascending order
    SortCriteria = new TaskField
                   {
                     TaskSummaryField = new TaskSummaryField
                                        {
                                          Field = TaskSummaryEnumField.TaskId,
                                        },
                   };
    IsSortAscending = true;
  }

  /// <summary>
  ///   By default UseDetailedVersion is set to false, meaning that the summary version of the ListTasksAsync API will be
  ///   called
  ///   and the TasksSummaries property will be populated in the TaskPage instance.
  ///   It is set to true when the extension method AsTaskDetailed() is called on the queryable instance, then the detailed
  ///   version of the ListTasksAsync API will be called and the TaskDetails property will be populated in the TaskPage
  ///   instance.
  /// </summary>
  public bool UseDetailedVersion { get; set; }

  protected override bool IsWhereExpressionTreeVisitorInstantiated
    => whereVisitor_ != null;

  protected override WhereExpressionTreeVisitor<TaskField, Filters, FiltersAnd, FilterField> WhereExpressionTreeVisitor
  {
    get
    {
      whereVisitor_ = whereVisitor_ ?? new TaskSummaryWhereExpressionTreeVisitor();
      return whereVisitor_;
    }
  }

  protected override OrderByExpressionTreeVisitor<TaskField> OrderByWhereExpressionTreeVisitor
  {
    get
    {
      orderByVisitor_ = orderByVisitor_ ?? new TaskSummaryOrderByExpressionTreeVisitor();
      return orderByVisitor_;
    }
  }

  protected override bool HandleMethodCallExpression(MethodCallExpression call)
  {
    if (call.Method.Name == nameof(QueryableExt.AsTaskDetailed))
    {
      UseDetailedVersion = true;
      return true;
    }

    return false;
  }
}
