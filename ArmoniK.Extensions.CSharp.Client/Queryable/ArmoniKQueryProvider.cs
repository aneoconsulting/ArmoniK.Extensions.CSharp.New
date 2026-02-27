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
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extensions.CSharp.Client.Queryable;

/// <summary>
///   Class query provider that build the protobuf filtering structure
/// </summary>
/// <typeparam name="TService">The service type that actually executes the query</typeparam>
/// <typeparam name="TPage">The type of page of elements returned by the query.</typeparam>
/// <typeparam name="TSource">The type of the requested instances.</typeparam>
/// <typeparam name="TField">The type of the paginated instances.</typeparam>
/// <typeparam name="TFilterOr">The type of the protobuf instance that represents a logical OR node.</typeparam>
/// <typeparam name="TFilterAnd">The type of the protobuf instance that represents a logical AND node.</typeparam>
/// <typeparam name="TFilterField">The type of the protobuf instance that describe a filter on a single property.</typeparam>
internal abstract class ArmoniKQueryProvider<TService, TPage, TSource, TField, TFilterOr, TFilterAnd, TFilterField> : IAsyncQueryProvider<TSource>
  where TField : new()
  where TFilterOr : new()
  where TFilterAnd : new()
{
  protected readonly ILogger<TService> Logger;

  /// <summary>
  ///   Create the query provider
  /// </summary>
  /// <param name="logger">The logger</param>
  protected ArmoniKQueryProvider(ILogger<TService> logger)
    => Logger = logger;

  public QueryExecution<TPage, TSource, TField, TFilterOr, TFilterAnd, TFilterField>? QueryExecution { get; private set; }

  /// <summary>
  ///   Create the query object
  /// </summary>
  /// <param name="expression">The filtering expression</param>
  /// <returns>The query object</returns>
  public IQueryable CreateQuery(Expression expression)
    => new ArmoniKQueryable<TSource>(this,
                                     expression);

  /// <summary>
  ///   Create the query object
  /// </summary>
  /// <param name="expression">The filtering expression</param>
  /// <returns>The query object</returns>
  public IQueryable<TElement> CreateQuery<TElement>(Expression expression)
    => new ArmoniKQueryable<TElement>(this,
                                      expression);

  /// <summary>
  ///   Visit the expression tree and generate the protobuf filtering structures
  ///   and fetches the instances according to the filter.
  /// </summary>
  /// <param name="expression">The filtering expression</param>
  /// <returns>An enumeration of instances compliant with the filter</returns>
  /// <exception cref="InvalidExpressionException">When the filtering expression is invalid</exception>
  public object? Execute(Expression expression)
  {
    QueryExecution = CreateQueryExecution();
    QueryExecution.VisitExpression(expression);
    if (QueryExecution.FuncReturnTSource != null)
    {
      return QueryExecution.FuncReturnTSource(QueryExecution.ExecuteAsync()
                                                            .Cast<TSource>());
    }

    if (QueryExecution.FuncReturnNullableTSource != null)
    {
      return QueryExecution.FuncReturnNullableTSource(QueryExecution.ExecuteAsync()
                                                                    .Cast<TSource>());
    }

    return QueryExecution.ExecuteAsync();
  }

  /// <summary>
  ///   Visit the expression tree and generate the protobuf filtering structures
  ///   and fetches the instances according to the filter.
  /// </summary>
  /// <param name="expression">The filtering expression</param>
  /// <returns>An enumeration of instances compliant with the filter</returns>
  /// <exception cref="InvalidExpressionException">When the filtering expression is invalid</exception>
  public TResult? Execute<TResult>(Expression expression)
    => (TResult?)Execute(expression);

  /// <summary>
  ///   Visit the expression tree and generate the protobuf filtering structures
  ///   and fetches the instances according to the filter.
  /// </summary>
  /// <param name="expression">The filtering expression</param>
  /// <param name="cancellationToken">The cancellation token</param>
  /// <returns>An asynchronous enumeration of instances compliant with the filter</returns>
  /// <exception cref="InvalidExpressionException">When the filtering expression is invalid</exception>
  public IAsyncEnumerable<TSource> ExecuteAsync(Expression        expression,
                                                CancellationToken cancellationToken = default)
  {
    QueryExecution = CreateQueryExecution();
    QueryExecution.VisitExpression(expression);
    return QueryExecution.ExecuteAsync(cancellationToken)
                         .Cast<TSource>();
  }

  protected abstract QueryExecution<TPage, TSource, TField, TFilterOr, TFilterAnd, TFilterField> CreateQueryExecution();
}
