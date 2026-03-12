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
using System.Linq;
using System.Linq.Expressions;

using ArmoniK.Utils;

namespace ArmoniK.Extensions.CSharp.Client.Queryable;

/// <summary>
///   Abstract base class for visiting and analyzing LINQ query expression trees.
///   Converts LINQ expressions into filter and sort criteria for ArmoniK queries.
/// </summary>
/// <typeparam name="TSource">The source type being queried.</typeparam>
/// <typeparam name="TField">The protobuf instance describing a field.</typeparam>
/// <typeparam name="TFilterOr">The type representing OR filter operations.</typeparam>
/// <typeparam name="TFilterAnd">The type representing AND filter operations.</typeparam>
/// <typeparam name="TFilterField">The type representing individual field filters.</typeparam>
internal abstract class QueryExpressionTreeVisitor<TSource, TField, TFilterOr, TFilterAnd, TFilterField>
  where TField : new()
  where TFilterOr : new()
  where TFilterAnd : new()
{
  public const int MAX_PAGE_SIZE = 1000;

  /// <summary>
  ///   Succession of methods which need to be applied successively on the result of the query.
  /// </summary>
  public List<ExtensionMethod> ExtensionMethods { get; } = new();

  /// <summary>
  ///   Final method applied on the result of the request which returns a scalar object
  /// </summary>
  public Func<IAsyncEnumerable<TSource>, object?>? FunctionReturningScalar { get; private set; }

  public TFilterOr? Filters { get; private set; } = new();

  /// <summary>
  ///   The protobuf instance describing how to sort the results of the query.
  /// </summary>
  public TField SortCriteria { get; protected set; } = new();

  public bool IsSortAscending { get; protected set; }

  public int? PageSize  { get; protected set; }
  public int? PageIndex { get; protected set; }

  protected abstract WhereExpressionTreeVisitor<TField, TFilterOr, TFilterAnd, TFilterField> WhereExpressionTreeVisitor        { get; }
  protected abstract OrderByExpressionTreeVisitor<TField>                                    OrderByWhereExpressionTreeVisitor { get; }

  /// <summary>
  ///   Visits and analyzes the provided expression tree.
  /// </summary>
  /// <param name="tree">The expression tree to analyze.</param>
  public void VisitTree(Expression tree)
  {
    VisitTreeInternal(tree);
    PrepareMethods();
    if (WhereExpressionTreeVisitor.Visited)
    {
      // A Where() was found
      Filters = WhereExpressionTreeVisitor.GetFilterOrRootNode();
    }
  }

  /// <summary>
  ///   Handle specific implementation of IQueryable extension methods.
  /// </summary>
  /// <param name="call">the call expression</param>
  /// <returns>Whether the method call has been handled</returns>
  protected virtual bool HandleMethodCallExpression(MethodCallExpression call)
    // By default, no specific implementation of IQueryable extension method is handled.
    => false;

  private void VisitTreeInternal(Expression tree)
  {
    if (tree is MethodCallExpression call)
    {
      if (call.Method.DeclaringType == typeof(System.Linq.Queryable) || call.Method.DeclaringType == typeof(QueryableExt))
      {
        var thisType = call.Arguments[0].Type;
        if (thisType == typeof(IQueryable<TSource>) || thisType == typeof(IOrderedQueryable<TSource>))
        {
          // This is a call of an extension method of IQueryable<TElement>,
          // then lets left recursively walk along the expression tree.
          VisitTreeInternal(call.Arguments[0]);
        }
        else if (thisType == typeof(ArmoniKQueryable<TSource>))
        {
          // This is the leftmost element of the LINQ query, it represents the entire collection.
        }
        else
        {
          // Should never happen
          throw new InvalidOperationException("Internal error: unexpected expression type");
        }

        if (call.Method.ReturnType == typeof(IQueryable<TSource>))
        {
          HandleIQueryableOfTSourceExpression(call);
        }
        else if (call.Method.ReturnType == typeof(TSource))
        {
          HandleTSourceExpression(call);
        }
        else if (call.Method.ReturnType == typeof(bool))
        {
          HandleBoolExpression(call);
        }
        else if (call.Method.Name.StartsWith(nameof(System.Linq.Queryable.OrderBy)))
        {
          // IOrderedQueryable<TSource> OrderBy<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
          // IOrderedQueryable<TSource> OrderBy<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, IComparer<TKey> comparer)
          // IOrderedQueryable<TSource> OrderByDescending<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
          // IOrderedQueryable<TSource> OrderByDescending<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, IComparer<TKey> comparer)
          var expression = (UnaryExpression)call.Arguments[1];
          var lambda     = (LambdaExpression)expression.Operand;

          IsSortAscending = call.Method.Name == nameof(System.Linq.Queryable.OrderBy);
          SortCriteria    = OrderByWhereExpressionTreeVisitor.Visit(lambda);
        }
        else if (!HandleMethodCallExpression(call))
        {
          // IQueryable<TResult> Select<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, int, TResult>> selector)
          // IQueryable<TResult> Select<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
          // IQueryable<TResult> OfType<TResult>(this IQueryable source)
          // IQueryable<TResult> Cast<TResult>(this IQueryable source)
          // IQueryable<TResult> SelectMany<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, IEnumerable<TResult>>> selector)
          // IQueryable<TResult> SelectMany<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, int, IEnumerable<TResult>>> selector)
          // IQueryable<TResult> SelectMany<TSource, TCollection, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, int, IEnumerable<TCollection>>> collectionSelector, Expression<Func<TSource, TCollection, TResult>> resultSelector)
          // IQueryable<TResult> SelectMany<TSource, TCollection, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, IEnumerable<TCollection>>> collectionSelector, Expression<Func<TSource, TCollection, TResult>> resultSelector)
          // IQueryable<TResult> Join<TOuter, TInner, TKey, TResult>(this IQueryable<TOuter> outer, IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector, Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, TInner, TResult>> resultSelector)
          // IQueryable<TResult> Join<TOuter, TInner, TKey, TResult>(this IQueryable<TOuter> outer, IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector, Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, TInner, TResult>> resultSelector, IEqualityComparer<TKey> comparer)
          // IQueryable<TResult> GroupJoin<TOuter, TInner, TKey, TResult>(this IQueryable<TOuter> outer, IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector, Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, IEnumerable<TInner>, TResult>> resultSelector)
          // IQueryable<TResult> GroupJoin<TOuter, TInner, TKey, TResult>(this IQueryable<TOuter> outer, IEnumerable<TInner> inner, Expression<Func<TOuter, TKey>> outerKeySelector, Expression<Func<TInner, TKey>> innerKeySelector, Expression<Func<TOuter, IEnumerable<TInner>, TResult>> resultSelector, IEqualityComparer<TKey> comparer)
          // IQueryable<TResult> GroupBy<TSource, TKey, TElement, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TSource, TElement>> elementSelector, Expression<Func<TKey, IEnumerable<TElement>, TResult>> resultSelector)
          // IQueryable<TResult> GroupBy<TSource, TKey, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TKey, IEnumerable<TSource>, TResult>> resultSelector)
          // IQueryable<TResult> GroupBy<TSource, TKey, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TKey, IEnumerable<TSource>, TResult>> resultSelector, IEqualityComparer<TKey> comparer)
          // IQueryable<TResult> GroupBy<TSource, TKey, TElement, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TSource, TElement>> elementSelector, Expression<Func<TKey, IEnumerable<TElement>, TResult>> resultSelector, IEqualityComparer<TKey> comparer)
          // IQueryable<IGrouping<TKey, TSource>> GroupBy<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector)
          // IQueryable<IGrouping<TKey, TElement>> GroupBy<TSource, TKey, TElement>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TSource, TElement>> elementSelector)
          // IQueryable<IGrouping<TKey, TSource>> GroupBy<TSource, TKey>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, IEqualityComparer<TKey> comparer)
          // IQueryable<IGrouping<TKey, TElement>> GroupBy<TSource, TKey, TElement>(this IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, Expression<Func<TSource, TElement>> elementSelector, IEqualityComparer<TKey> comparer)
          // IQueryable<(TFirst First, TSecond Second)> Zip<TFirst, TSecond>(this IQueryable<TFirst> source1, IEnumerable<TSecond> source2)
          // IQueryable<TResult> Zip<TFirst, TSecond, TResult>(this IQueryable<TFirst> source1, IEnumerable<TSecond> source2, Expression<Func<TFirst, TSecond, TResult>> resultSelector)
          // int Count<TSource>(this IQueryable<TSource> source)
          // int Count<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
          // long LongCount<TSource>(this IQueryable<TSource> source)
          // long LongCount<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
          // TResult Min<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
          // TResult Max<TSource, TResult>(this IQueryable<TSource> source, Expression<Func<TSource, TResult>> selector)
          // int Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int>> selector)
          // int? Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int?>> selector)
          // long Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long>> selector)
          // long? Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long?>> selector)
          // float Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float>> selector)
          // float? Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float?>> selector)
          // double Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double>> selector)
          // double? Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double?>> selector)
          // decimal Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal>> selector)
          // decimal? Sum<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal?>> selector)
          // double Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int>> selector)
          // double? Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int?>> selector)
          // float Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float>> selector)
          // float? Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, float?>> selector)
          // double Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long>> selector)
          // double? Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, long?>> selector)
          // double Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double>> selector)
          // double? Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, double?>> selector)
          // decimal Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal>> selector)
          // decimal? Average<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, decimal?>> selector)
          // TAccumulate Aggregate<TSource, TAccumulate>(this IQueryable<TSource> source, TAccumulate seed, Expression<Func<TAccumulate, TSource, TAccumulate>> func)
          // TResult Aggregate<TSource, TAccumulate, TResult>(this IQueryable<TSource> source, TAccumulate seed, Expression<Func<TAccumulate, TSource, TAccumulate>> func, Expression<Func<TAccumulate, TResult>> selector)
          throw new
            InvalidOperationException($"Extension method IQueryable<{typeof(TSource).Name}>.{call.Method.Name} is not supported. Please use the enumerable alternative instead.");
        }
      }
    }
  }

  private void HandleBoolExpression(MethodCallExpression call)
  {
    if (call.Method.Name == nameof(System.Linq.Queryable.Contains))
    {
      var item = (TSource)call.Arguments[1]
                              .EvaluateExpression()!;
      if (call.Arguments.Count == 2)
      {
        // bool Contains<TSource>(this IQueryable<TSource> source, TSource item)
        FunctionReturningScalar = enumerable => enumerable.ContainsAsync(item)
                                                          .WaitSync();
      }
      else
      {
        // bool Contains<TSource>(this IQueryable<TSource> source, TSource item, IEqualityComparer<TSource> comparer)
        var comparer = (IEqualityComparer<TSource>)call.Arguments[2]
                                                       .EvaluateExpression()!;
        FunctionReturningScalar = enumerable => enumerable.ContainsAsync(item,
                                                                         comparer)
                                                          .WaitSync();
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.SequenceEqual))
    {
      var source2 = (IEnumerable<TSource>)call.Arguments[1]
                                              .EvaluateExpression()!;
      if (call.Arguments.Count == 2)
      {
        // bool SequenceEqual<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
        FunctionReturningScalar = enumerable => enumerable.SequenceEqualAsync(source2.ToAsyncEnumerable())
                                                          .WaitSync();
      }
      else
      {
        // bool SequenceEqual<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2, IEqualityComparer<TSource> comparer)
        var comparer = (IEqualityComparer<TSource>)call.Arguments[2]
                                                       .EvaluateExpression()!;
        FunctionReturningScalar = enumerable => enumerable.SequenceEqualAsync(source2.ToAsyncEnumerable(),
                                                                              comparer)
                                                          .WaitSync();
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.Any))
    {
      if (call.Arguments.Count == 1)
      {
        // bool Any<TSource>(this IQueryable<TSource> source)
        FunctionReturningScalar = enumerable => enumerable.AnyAsync()
                                                          .WaitSync();
      }
      else
      {
        // bool Any<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        var expression = (UnaryExpression)call.Arguments[1];
        var lambda     = (LambdaExpression)expression.Operand;
        var func       = (Func<TSource, bool>)lambda.EvaluateExpression()!;
        FunctionReturningScalar = enumerable => enumerable.AnyAsync(func)
                                                          .WaitSync();
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.All))
    {
      // bool All<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
      var expression = (UnaryExpression)call.Arguments[1];
      var lambda     = (LambdaExpression)expression.Operand;
      var func       = (Func<TSource, bool>)lambda.EvaluateExpression()!;
      FunctionReturningScalar = enumerable => enumerable.AllAsync(func)
                                                        .WaitSync();
    }
    else
    {
      throw new
        InvalidOperationException($"Extension method IQueryable<{typeof(TSource).Name}>.{call.Method.Name} is not supported. Please use the enumerable alternative instead.");
    }
  }

  private void HandleTSourceExpression(MethodCallExpression call)
  {
    if (call.Method.Name.StartsWith(nameof(System.Linq.Queryable.First)))
    {
      if (call.Arguments.Count == 1)
      {
        // TSource First<TSource>(this IQueryable<TSource> source)
        // TSource FirstOrDefault<TSource>(this IQueryable<TSource> source)
        // Nothing to do here.
      }
      else
      {
        // TSource First<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        // TSource FirstOrDefault<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        var expression = (UnaryExpression)call.Arguments[1];
        var lambda     = (LambdaExpression)expression.Operand;
        ExtensionMethods.Add(new WhereMethod(WhereExpressionTreeVisitor,
                                             lambda));
      }

      FunctionReturningScalar = call.Method.Name == nameof(System.Linq.Queryable.FirstOrDefault)
                                  ? enumerable => enumerable.FirstOrDefaultAsync()
                                                            .WaitSync()
                                  : enumerable => enumerable.FirstAsync()
                                                            .WaitSync();
    }
    else if (call.Method.Name.StartsWith(nameof(System.Linq.Queryable.Last)))
    {
      if (call.Arguments.Count == 1)
      {
        // TSource Last<TSource>(this IQueryable<TSource> source)
        // TSource LastOrDefault<TSource>(this IQueryable<TSource> source)
        // Nothing to do here.
      }
      else
      {
        // TSource Last<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        // TSource LastOrDefault<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        var expression = (UnaryExpression)call.Arguments[1];
        var lambda     = (LambdaExpression)expression.Operand;
        ExtensionMethods.Add(new WhereMethod(WhereExpressionTreeVisitor,
                                             lambda));
      }

      FunctionReturningScalar = call.Method.Name == nameof(System.Linq.Queryable.LastOrDefault)
                                  ? enumerable => enumerable.LastOrDefaultAsync()
                                                            .WaitSync()
                                  : enumerable => enumerable.LastAsync()
                                                            .WaitSync();
    }
    else if (call.Method.Name.StartsWith(nameof(System.Linq.Queryable.Single)))
    {
      if (call.Arguments.Count == 1)
      {
        // TSource Single<TSource>(this IQueryable<TSource> source)
        // TSource SingleOrDefault<TSource>(this IQueryable<TSource> source)
        // Nothing to do here.
      }
      else
      {
        // TSource Single<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        // TSource SingleOrDefault<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        var expression = (UnaryExpression)call.Arguments[1];
        var lambda     = (LambdaExpression)expression.Operand;
        ExtensionMethods.Add(new WhereMethod(WhereExpressionTreeVisitor,
                                             lambda));
      }

      FunctionReturningScalar = call.Method.Name == nameof(System.Linq.Queryable.SingleOrDefault)
                                  ? enumerable => enumerable.SingleOrDefaultAsync()
                                                            .WaitSync()
                                  : enumerable => enumerable.SingleAsync()
                                                            .WaitSync();
    }
    else if (call.Method.Name.StartsWith(nameof(System.Linq.Queryable.ElementAt)))
    {
      // TSource ElementAt<TSource>(this IQueryable<TSource> source, int index)
      // TSource ElementAtOrDefault<TSource>(this IQueryable<TSource> source, int index)
      var index = (int?)call.Arguments[1]
                            .EvaluateExpression();
      if (call.Method.Name == nameof(System.Linq.Queryable.ElementAt))
      {
        FunctionReturningScalar = enumerable => enumerable.ElementAtAsync(index!.Value)
                                                          .WaitSync();
      }
      else
      {
        FunctionReturningScalar = enumerable => enumerable.ElementAtOrDefaultAsync(index!.Value)
                                                          .WaitSync();
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.Aggregate))
    {
      // TSource Aggregate<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, TSource, TSource>> func)
      var expression = (UnaryExpression)call.Arguments[1];
      var func       = (Func<TSource, TSource, TSource>)expression.Operand.EvaluateExpression()!;
      FunctionReturningScalar = enumerable => enumerable.AggregateAsync(func)
                                                        .WaitSync();
    }
    else
    {
      // TSource Min<TSource>(this IQueryable<TSource> source)
      // TSource Max<TSource>(this IQueryable<TSource> source)
      throw new
        InvalidOperationException($"Extension method IQueryable<{typeof(TSource).Name}>.{call.Method.Name} is not supported. Please use the enumerable alternative instead.");
    }
  }

  private void HandleIQueryableOfTSourceExpression(MethodCallExpression call)
  {
    if (call.Method.Name == nameof(System.Linq.Queryable.Where))
    {
      var expression = (UnaryExpression)call.Arguments[1];
      var lambda     = (LambdaExpression)expression.Operand;
      ExtensionMethods.Add(new WhereMethod(WhereExpressionTreeVisitor,
                                           lambda));
    }
    else if (call.Method.Name == nameof(QueryableExt.WithPageSize))
    {
      // Custom extension method that sets ArmoniK filter's page size.
      var expression = (ConstantExpression)call.Arguments[1];
      PageSize = (int)expression.Value;
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.Skip))
    {
      // IQueryable<TSource> Skip<TSource>(this IQueryable<TSource> source, int count)
      var count = ((int?)call.Arguments[1]
                             .EvaluateExpression())!.Value;
      ExtensionMethods.Add(new SkipMethod(count));
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.Take))
    {
      // IQueryable<TSource> Take<TSource>(this IQueryable<TSource> source, int count)
      var count = ((int?)call.Arguments[1]
                             .EvaluateExpression())!.Value;
      ExtensionMethods.Add(new TakeMethod(count));
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.TakeWhile))
    {
      // IQueryable<TSource> TakeWhile<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
      // IQueryable<TSource> TakeWhile<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int, bool>> predicate)
      var expression = (UnaryExpression)call.Arguments[1];
      var lambda     = (LambdaExpression)expression.Operand;
      if (lambda.Parameters.Count == 1)
      {
        var func = (Func<TSource, bool>)lambda.EvaluateExpression()!;
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.TakeWhile(func)));
      }
      else
      {
        var func = (Func<TSource, int, bool>)lambda.EvaluateExpression()!;
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.TakeWhile(func)));
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.SkipWhile))
    {
      // IQueryable<TSource> SkipWhile<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
      // IQueryable<TSource> SkipWhile<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int, bool>> predicate)
      var expression = (UnaryExpression)call.Arguments[1];
      var lambda     = (LambdaExpression)expression.Operand;
      if (lambda.Parameters.Count == 1)
      {
        var func = (Func<TSource, bool>)lambda.EvaluateExpression()!;
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.SkipWhile(func)));
      }
      else
      {
        var func = (Func<TSource, int, bool>)lambda.EvaluateExpression()!;
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.SkipWhile(func)));
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.Distinct))
    {
      // IQueryable<TSource> Distinct<TSource>(this IQueryable<TSource> source)
      // IQueryable<TSource> Distinct<TSource>(this IQueryable<TSource> source, IEqualityComparer<TSource> comparer)
      if (call.Arguments.Count == 1)
      {
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.Distinct()));
      }
      else
      {
        var comparer = (IEqualityComparer<TSource>)call.Arguments[1]
                                                       .EvaluateExpression()!;
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.Distinct(comparer)));
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.Concat))
    {
      // IQueryable<TSource> Concat<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
      var source2 = (IEnumerable<TSource>)call.Arguments[1]
                                              .EvaluateExpression()!;
      ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.Concat(source2.ToAsyncEnumerable())));
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.Union))
    {
      // IQueryable<TSource> Union<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
      // IQueryable<TSource> Union<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2, IEqualityComparer<TSource> comparer)
      var source2 = (IEnumerable<TSource>)call.Arguments[1]
                                              .EvaluateExpression()!;
      if (call.Arguments.Count == 2)
      {
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.Union(source2.ToAsyncEnumerable())));
      }
      else
      {
        var comparer = (IEqualityComparer<TSource>)call.Arguments[2]
                                                       .EvaluateExpression()!;
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.Union(source2.ToAsyncEnumerable(),
                                                                                      comparer)));
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.Intersect))
    {
      // IQueryable<TSource> Intersect<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
      // IQueryable<TSource> Intersect<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2, IEqualityComparer<TSource> comparer)
      var source2 = (IEnumerable<TSource>)call.Arguments[1]
                                              .EvaluateExpression()!;
      if (call.Arguments.Count == 2)
      {
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.Intersect(source2.ToAsyncEnumerable())));
      }
      else
      {
        var comparer = (IEqualityComparer<TSource>)call.Arguments[1]
                                                       .EvaluateExpression()!;
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.Intersect(source2.ToAsyncEnumerable(),
                                                                                          comparer)));
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.Except))
    {
      // IQueryable<TSource> Except<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
      // IQueryable<TSource> Except<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2, IEqualityComparer<TSource> comparer)
      var source2 = (IEnumerable<TSource>)call.Arguments[1]
                                              .EvaluateExpression()!;
      if (call.Arguments.Count == 2)
      {
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.Except(source2.ToAsyncEnumerable())));
      }
      else
      {
        var comparer = (IEqualityComparer<TSource>)call.Arguments[1]
                                                       .EvaluateExpression()!;
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.Except(source2.ToAsyncEnumerable(),
                                                                                       comparer)));
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.DefaultIfEmpty))
    {
      // IQueryable<TSource> DefaultIfEmpty<TSource>(this IQueryable<TSource> source)
      // IQueryable<TSource> DefaultIfEmpty<TSource>(this IQueryable<TSource> source, TSource defaultValue)
      if (call.Arguments.Count == 1)
      {
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.DefaultIfEmpty()!));
      }
      else
      {
        var value = (TSource)call.Arguments[1]
                                 .EvaluateExpression()!;
        ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.DefaultIfEmpty(value)));
      }
    }
    else if (call.Method.Name == nameof(System.Linq.Queryable.Reverse))
    {
      // IQueryable<TSource> Reverse<TSource>(this IQueryable<TSource> source)
      ExtensionMethods.Add(new SimpleExtensionMethod(enumerable => enumerable.Reverse()));
    }
    else
    {
      // IQueryable<TSource> Concat<TSource>(this IQueryable<TSource> source1, IEnumerable<TSource> source2)
      throw new
        InvalidOperationException($"Extension method IQueryable<{typeof(TSource).Name}>.{call.Method.Name} is not supported. Please use the enumerable alternative instead.");
    }
  }

  /// <summary>
  ///   Executes the extension methods on the result of the gRPC request
  /// </summary>
  /// <param name="source">The result of the gRPC request</param>
  /// <returns>The final result</returns>
  public IAsyncEnumerable<TSource> RunAllExtensionsMethods(IAsyncEnumerable<TSource> source)
  {
    foreach (var method in ExtensionMethods)
    {
      if (method.Function != null)
      {
        source = method.Function(source);
      }
    }

    return source;
  }

  /// <summary>
  ///   Prepare the requests
  /// </summary>
  public void PrepareMethods()
  {
    if (!PageSize.HasValue)
    {
      PageSize = MAX_PAGE_SIZE;
    }

    ExtensionMethod? previous = null;
    foreach (var method in ExtensionMethods)
    {
      if (previous is SkipMethod skipMethod && method is TakeMethod takeMethod && takeMethod.Count > 0 && skipMethod.Count >= takeMethod.Count)
      {
        if (skipMethod.Count % takeMethod.Count == 0 && takeMethod.Count == PageSize)
        {
          PageIndex = skipMethod.Count / takeMethod.Count;
        }
      }

      method.Prepare(previous);
      previous = method;
    }
  }

  internal abstract class ExtensionMethod
  {
    public Func<IAsyncEnumerable<TSource>, IAsyncEnumerable<TSource>>? Function { get; protected set; }

    public virtual void Prepare(ExtensionMethod? previous)
    {
    }
  }

  private class SimpleExtensionMethod : ExtensionMethod
  {
    public SimpleExtensionMethod(Func<IAsyncEnumerable<TSource>, IAsyncEnumerable<TSource>> func)
      => Function = func;
  }

  private class SkipMethod : ExtensionMethod
  {
    public SkipMethod(int count)
    {
      Function = enumeration => enumeration.Skip(count);
      Count    = count;
    }

    public int Count { get; }
  }

  private class TakeMethod : ExtensionMethod
  {
    public TakeMethod(int count)
    {
      Function = enumeration => enumeration.Take(count);
      Count    = count;
    }

    public int Count { get; }
  }

  private class WhereMethod : ExtensionMethod
  {
    private readonly LambdaExpression                                                        lambda_;
    private readonly WhereExpressionTreeVisitor<TField, TFilterOr, TFilterAnd, TFilterField> whereVisitor_;

    public WhereMethod(WhereExpressionTreeVisitor<TField, TFilterOr, TFilterAnd, TFilterField> visitor,
                       LambdaExpression                                                        lambda)
    {
      whereVisitor_ = visitor;
      lambda_       = lambda;
    }

    public override void Prepare(ExtensionMethod? previous)
    {
      if (lambda_.Parameters.Count == 1)
      {
        // IQueryable<TSource> Where<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
        if (previous?.Function == null)
        {
          whereVisitor_.Visit(lambda_);
        }
        else
        {
          var func = (Func<TSource, bool>)lambda_.EvaluateExpression()!;
          Function = enumerable => enumerable.Where(func);
        }
      }
      else
      {
        // IQueryable<TSource> Where<TSource>(this IQueryable<TSource> source, Expression<Func<TSource, int, bool>> predicate)
        var func = (Func<TSource, int, bool>)lambda_.EvaluateExpression()!;
        Function = enumeration => enumeration.Where(func);
      }
    }
  }
}
