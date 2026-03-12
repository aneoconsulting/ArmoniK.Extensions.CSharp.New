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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;

using ArmoniK.Utils;

using Microsoft.Extensions.Logging;

namespace ArmoniK.Extensions.CSharp.Client.Queryable;

/// <summary>
///   Class that define a query object
/// </summary>
/// <typeparam name="TSource"></typeparam>
internal class ArmoniKQueryable<TSource> : IOrderedQueryable<TSource>, IAsyncEnumerable<TSource>
{
  private readonly ILogger logger_;

  /// <summary>
  ///   Create the query object
  /// </summary>
  /// <param name="provider">The query provider</param>
  /// <param name="logger">The logger</param>
  /// <exception cref="ArgumentNullException">When provider is null</exception>
  public ArmoniKQueryable(IAsyncQueryProvider<TSource> provider,
                          ILogger                      logger)
  {
    Provider   = provider ?? throw new ArgumentNullException(nameof(provider));
    Expression = Expression.Constant(this);
    logger_    = logger ?? throw new ArgumentNullException(nameof(logger));
  }

  /// <summary>
  ///   Create the query object
  /// </summary>
  /// <param name="provider">The query provider</param>
  /// <param name="tree">The filtering tree</param>
  /// <param name="logger">The logger</param>
  /// <exception cref="ArgumentNullException">When provider or tree is null</exception>
  public ArmoniKQueryable(IQueryProvider provider,
                          Expression     tree,
                          ILogger        logger)
  {
    Provider   = provider ?? throw new ArgumentNullException(nameof(provider));
    Expression = tree     ?? throw new ArgumentNullException(nameof(tree));
    logger_    = logger   ?? throw new ArgumentNullException(nameof(logger));
  }

  /// <summary>
  ///   Makes the query object asynchronously enumerable
  /// </summary>
  public IAsyncEnumerator<TSource> GetAsyncEnumerator(CancellationToken cancellationToken = default)
  {
    var asyncProvider = (IAsyncQueryProvider<TSource>)Provider;
    return asyncProvider.ExecuteAsync(Expression,
                                      cancellationToken)
                        .GetAsyncEnumerator(cancellationToken);
  }

  /// <inheritdoc />
  public Type ElementType
    => typeof(TSource);

  /// <inheritdoc />
  public Expression Expression { get; }

  /// <inheritdoc />
  public IQueryProvider Provider { get; }

  /// <inheritdoc />
  public IEnumerator<TSource> GetEnumerator()
  {
    var result = Provider.Execute(Expression);
    if (result is IAsyncEnumerable<object> asyncEnumerable)
    {
      var name = typeof(TSource).Name;
      logger_.LogWarning("Do not use an extension method which implicitly converts an IQueryable<{Name}> into an IEnumerable<{Name}> (like ToAsyncEnumerable()) and use AsAsyncEnumerable() instead.",
                         name,
                         name);
      return asyncEnumerable.Cast<TSource>()
                            .ToListAsync()
                            .WaitSync()
                            .GetEnumerator();
    }

    if (result is TSource scalar)
    {
      var array = new List<TSource>
                  {
                    scalar,
                  };
      return array.GetEnumerator();
    }

    throw new InvalidOperationException($"The query returned an unexpected instance type: {result.GetType()}");
  }

  IEnumerator IEnumerable.GetEnumerator()
    => GetEnumerator();
}
