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
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Blob;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Task;

using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using TaskStatus = ArmoniK.Extensions.CSharp.Common.Common.Domain.Task.TaskStatus;
using Type = System.Type;

namespace ArmoniK.Extensions.CSharp.Client.Queryable;

/// <summary>
///   Visitor class of a lambda describing a filter.
/// </summary>
/// <typeparam name="TField">The protobuf instance describing a field.</typeparam>
/// <typeparam name="TFilterOr">The type of logical OR nodes.</typeparam>
/// <typeparam name="TFilterAnd">The type of logical AND nodes.</typeparam>
/// <typeparam name="TFilterField">The type of instance node describing a filter on a single property.</typeparam>
internal abstract class WhereExpressionTreeVisitor<TField, TFilterOr, TFilterAnd, TFilterField>
  where TFilterOr : new()
  where TFilterAnd : new()
{
  protected readonly Stack<(object, Type)> FilterStack = new();

  /// <summary>
  ///   Visit the lambda Expression Tree describing the filter.
  /// </summary>
  /// <param name="lambda">the lambda Expression Tree.</param>
  public void Visit(LambdaExpression lambda)
  {
    Visit(lambda.Body);

    if (FilterStack.Count == 2)
    {
      // merge : left && right
      HandleBoolExpression(ExpressionType.AndAlso);
    }
    else if (FilterStack.Count == 1)
    {
      var (obj, _) = FilterStack.Pop();
      if (obj is bool result)
      {
        FilterStack.Push((result, typeof(bool)));
      }
      else
      {
        FilterStack.Push((CreateFilterFromStack(obj)!, typeof(bool)));
      }
    }
    else
    {
      throw new InvalidOperationException("Internal error: analysis stack is in an inconsistent state");
    }
  }

  /// <summary>
  ///   Get the resulting tree from the analysis stack.
  /// </summary>
  /// <returns>The root node of the tree.</returns>
  /// <exception cref="InvalidOperationException">
  ///   When the analysis stack is in an inconsistent state,
  ///   which results from an invalid Expression Tree filter.
  /// </exception>
  public TFilterOr? GetFilterOrRootNode()
  {
    var filters = FilterStack.Pop();
    if (FilterStack.Any())
    {
      throw new InvalidOperationException("Internal error: analysis stack is in an inconsistent state");
    }

    if (filters.Item1 is bool boolValue)
    {
      if (boolValue)
      {
        // true => empty filter
        return new TFilterOr();
      }

      // false => no result (and then no filter)
      return default;
    }

    return (TFilterOr)filters.Item1;
  }

  private TFilterOr CreateFilterFromStack(object filter)
  {
    var orNode = new TFilterOr();
    if (filter is TFilterOr filterOr)
    {
      orNode = filterOr;
    }
    else if (filter is TFilterAnd andNode)
    {
      GetRepeatedFilterAnd(orNode)
        .Add(andNode);
    }
    else if (filter is TFilterField filterField)
    {
      var and = new TFilterAnd();
      GetRepeatedFilterField(and)
        .Add(filterField);
      GetRepeatedFilterAnd(orNode)
        .Add(and);
    }

    return orNode;
  }

  private void Visit(Expression node,
                     bool       notOp = false)
  {
    if (node is MethodCallExpression call)
    {
      var hasParameterQualifier = call.Object!.IsLeftMostQualifierAParameter();
      var typeName              = call.Method.DeclaringType?.FullName ?? "";
      if (call.Method.Name == nameof(string.StartsWith) && typeName == "System.String" && hasParameterQualifier)
      {
        if (call.Arguments.Count != 1)
        {
          throw new InvalidExpressionException("Invalid filter: StartsWith method overload not supported.");
        }

        Visit(call.Object!);
        Visit(call.Arguments[0]);
        OnStringMethodOperator(call.Method);
        return;
      }

      if (call.Method.Name == nameof(string.EndsWith) && typeName == "System.String" && hasParameterQualifier)
      {
        if (call.Arguments.Count != 1)
        {
          throw new InvalidExpressionException("Invalid filter: EndsWith method overload not supported.");
        }

        Visit(call.Object!);
        Visit(call.Arguments[0]);
        OnStringMethodOperator(call.Method);
        return;
      }

      if (call.Method.Name == nameof(string.Contains))
      {
        if (typeName == "System.String" && hasParameterQualifier)
        {
          if (call.Arguments.Count != 1)
          {
            throw new InvalidExpressionException("Invalid filter: Contains method overload not supported.");
          }

          Visit(call.Object!);
          Visit(call.Arguments[0]);
          OnStringMethodOperator(call.Method,
                                 notOp);
          return;
        }

        if (call.Object != null)
        {
          TypeFilter filter = (t,
                               criteria) => t.FullName?.StartsWith("System.Collections.Generic.IEnumerable`1") ?? false;

          var ienumerable = call.Object.Type.FindInterfaces(filter,
                                                            null)
                                .FirstOrDefault();
          if (ienumerable != null)
          {
            var val = call.Object.EvaluateExpression();
            if (val != null)
            {
              FilterStack.Push((val, val.GetType()));
            }

            Visit(call.Arguments[0]);

            VisitContainsMethod(call,
                                notOp);
            return;
          }
        }
        else if (typeName == "System.Linq.Enumerable")
        {
          if (call.Arguments.Count != 2)
          {
            throw new InvalidExpressionException("Invalid filter: Contains method overload not supported.");
          }

          Visit(call.Arguments[0]);
          Visit(call.Arguments[1]);

          VisitContainsMethod(call,
                              notOp);
          return;
        }
      }

      if (call.Method.Name == "get_Item")
      {
        Visit(call.Object!);
        Visit(call.Arguments[0]);
        OnIndexerAccess();
        return;
      }

      // Evaluate the method
      var result = call.EvaluateExpression();
      if (result != null)
      {
        FilterStack.Push((result, result.GetType()));
      }
    }
    else if (node is InvocationExpression invoke)
    {
      // Invocation of a delegate or lambda, let's evaluate it
      var result = invoke.EvaluateExpression();
      if (result != null)
      {
        FilterStack.Push((result, result.GetType()));
      }
    }
    else if (node is ConstantExpression constant)
    {
      OnConstant(constant);
    }
    else if (node is MemberExpression member)
    {
      switch (member.Member.MemberType)
      {
        case MemberTypes.Property:
          OnPropertyMemberAccess(member);
          break;
        case MemberTypes.Field:
          OnFieldAccess(member);
          break;
      }
    }
    else if (node is BinaryExpression binary)
    {
      if (ExpressionContainsLambdaParameter(binary.Left) || ExpressionContainsLambdaParameter(binary.Right))
      {
        Visit(binary.Left);
        Visit(binary.Right);
        OnBinaryOperator(binary.NodeType);
      }
      else
      {
        var result = binary.EvaluateExpression();
        if (result != null)
        {
          FilterStack.Push((result, result.GetType()));
        }
      }
    }
    else if (node is UnaryExpression unary)
    {
      if (unary.NodeType == ExpressionType.Convert)
      {
        Visit(unary.Operand);
      }
      else if (unary.NodeType == ExpressionType.Not && ExpressionContainsLambdaParameter(unary.Operand))
      {
        Visit(unary.Operand,
              !notOp);
      }
      else
      {
        var result = unary.EvaluateExpression();
        if (result != null)
        {
          FilterStack.Push((result, result.GetType()));
        }
      }
    }
  }

  private void OnPropertyMemberAccess(MemberExpression member)
  {
    if (member.IsLeftMostQualifierAParameter())
    {
      if (!PushProperty(member))
      {
        throw new InvalidOperationException("Unsupported filter expression on member " + member.Member.Name);
      }
    }
    else
    {
      var val = member.GetValueFromExpression();
      if (val != null)
      {
        FilterStack.Push((val, val.GetType()));
      }
      else
      {
        throw new InvalidOperationException("Unsupported filter expression on member " + member.Member.Name);
      }
    }
  }

  private void OnConstant(ConstantExpression constant)
    => FilterStack.Push((constant.Value, constant.Type));

  private bool OnFieldAccess(MemberExpression member)
  {
    var val = member.GetValueFromExpression();
    if (val != null)
    {
      FilterStack.Push((val, val.GetType()));
      return true;
    }

    return false;
  }

  private void OnBinaryOperator(ExpressionType expressionType)
  {
    var rhs     = FilterStack.Pop();
    var lhs     = FilterStack.Pop();
    var rhsType = rhs.Item2;
    var lhsType = lhs.Item2;
    FilterStack.Push(lhs);
    FilterStack.Push(rhs);
    if (rhsType == typeof(bool))
    {
      HandleBoolExpression(expressionType);
    }
    else if (rhsType == typeof(string))
    {
      HandleStringExpression(expressionType);
    }
    else if (rhsType == typeof(BlobStatus) || lhsType == typeof(BlobStatus))
    {
      HandleBlobStatusExpression(expressionType);
    }
    else if (rhsType == typeof(TaskStatus) || lhsType == typeof(TaskStatus))
    {
      HandleTaskStatusExpression(expressionType);
    }
    else if (rhsType == typeof(int))
    {
      HandleIntegerExpression(expressionType);
    }
    else if (rhsType == typeof(DateTime))
    {
      HandleDateTimeExpression(expressionType);
    }
    else if (rhsType == typeof(TimeSpan))
    {
      HandleTimeSpanExpression(expressionType);
    }
    else
    {
      throw new InvalidOperationException("Invalid filter: Operands of expression type {rhsType.Name} are not supported.");
    }
  }

  private static Expression GetLeftMostExpression(BinaryExpression binaryExpression)
  {
    if (binaryExpression.Left is BinaryExpression binary)
    {
      return GetLeftMostExpression(binary);
    }

    return binaryExpression.Left;
  }

  private static MemberExpression GetRightMostMemberExpression(MemberExpression member)
  {
    if (member.Expression is MemberExpression rightExpression)
    {
      return GetRightMostMemberExpression(rightExpression);
    }

    return member;
  }

  private bool ExpressionContainsLambdaParameter(Expression expression)
  {
    if (expression is UnaryExpression unary && unary.NodeType == ExpressionType.Convert)
    {
      expression = unary.Operand;
    }

    if (expression is BinaryExpression binary)
    {
      return ExpressionContainsLambdaParameter(binary.Left) || ExpressionContainsLambdaParameter(binary.Right);
    }

    if (expression is MemberExpression member)
    {
      member = GetRightMostMemberExpression(member);
      return ExpressionContainsLambdaParameter(member.Expression);
    }

    if (expression is MethodCallExpression call)
    {
      if (call.Method.DeclaringType?.FullName == "System.Linq.Enumerable")
      {
        // This is an extension method, <this> is then the first argument.
        foreach (var arg in call.Arguments)
        {
          if (ExpressionContainsLambdaParameter(arg))
          {
            return true;
          }
        }

        return false;
      }

      if (call.Object != null)
      {
        return ExpressionContainsLambdaParameter(call.Object);
      }
    }

    return expression.NodeType == ExpressionType.Parameter;
  }

  protected abstract TFilterOr  CreateFilterOr(params  TFilterAnd[]   filters);
  protected abstract TFilterAnd CreateFilterAnd(params TFilterField[] filters);

  protected abstract RepeatedField<TFilterAnd> GetRepeatedFilterAnd(TFilterOr or);

  protected abstract RepeatedField<TFilterField> GetRepeatedFilterField(TFilterAnd or);

  private void HandleBoolExpression(ExpressionType type)
  {
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
    switch (type)
    {
      case ExpressionType.OrElse:
        if (lhsFilter is TFilterOr lhsOrFilter)
        {
          if (rhsFilter is TFilterOr rhsOrFilter)
          {
            // <or expression> || <or expression>
            foreach (var rhsAndFilter in GetRepeatedFilterAnd(rhsOrFilter))
            {
              GetRepeatedFilterAnd(lhsOrFilter)
                .Add(rhsAndFilter);
            }

            FilterStack.Push((lhsOrFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <or expression> || <and expression>
            GetRepeatedFilterAnd(lhsOrFilter)
              .Add(rhsAndFilter);
            FilterStack.Push((lhsOrFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <or expression> || <filter field>
            var andFilter = CreateFilterAnd(rhsFilterField);
            GetRepeatedFilterAnd(lhsOrFilter)
              .Add(andFilter);
            FilterStack.Push((lhsOrFilter, typeof(bool)));
          }
          else if (rhsFilter is bool rhsValue)
          {
            // <or expression> || <bool>
            if (rhsValue)
            {
              FilterStack.Push((true, typeof(bool)));
            }
            else
            {
              FilterStack.Push((lhsOrFilter, typeof(bool)));
            }
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }
        else if (lhsFilter is TFilterAnd lhsAndFilter)
        {
          if (rhsFilter is TFilterOr rhsOrFilter)
          {
            // <and expression> || <or expression>
            var orFilter = CreateFilterOr(lhsAndFilter);
            foreach (var rhsAndFilter in GetRepeatedFilterAnd(rhsOrFilter))
            {
              GetRepeatedFilterAnd(orFilter)
                .Add(rhsAndFilter);
            }

            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <and expression> || <and expression>
            var orFilter = CreateFilterOr(lhsAndFilter,
                                          rhsAndFilter);
            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <and expression> || <filter field>
            var andRhsFilter = CreateFilterAnd(rhsFilterField);
            var orFilter = CreateFilterOr(lhsAndFilter,
                                          andRhsFilter);
            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else if (rhsFilter is bool rhsValue)
          {
            // <and expression> || <bool>
            if (rhsValue)
            {
              FilterStack.Push((true, typeof(bool)));
            }
            else
            {
              FilterStack.Push((lhsAndFilter, typeof(bool)));
            }
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }
        else if (lhsFilter is TFilterField lhsFilterField)
        {
          if (rhsFilter is TFilterOr rhsOrFilter)
          {
            // <filter field> || <or expression>
            var andFilter = CreateFilterAnd(lhsFilterField);
            var orFilter  = CreateFilterOr(andFilter);
            foreach (var rhsAndFilter in GetRepeatedFilterAnd(rhsOrFilter))
            {
              GetRepeatedFilterAnd(orFilter)
                .Add(rhsAndFilter);
            }

            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <filter field> || <and expression>
            var orFilter = CreateFilterOr(CreateFilterAnd(lhsFilterField),
                                          rhsAndFilter);
            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <filter field> || <filter field>
            var orFilter = CreateFilterOr(CreateFilterAnd(lhsFilterField),
                                          CreateFilterAnd(rhsFilterField));
            FilterStack.Push((orFilter!, typeof(bool)));
          }
          else if (rhsFilter is bool rhsValue)
          {
            // <filter field> || <bool>
            if (rhsValue)
            {
              FilterStack.Push((true, typeof(bool)));
            }
            else
            {
              FilterStack.Push((lhsFilterField, typeof(bool)));
            }
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }
        else if (lhsFilter is bool lhsValue)
        {
          // <bool> || <any>
          FilterStack.Push((lhsValue
                              ? true
                              : rhsFilter, typeof(bool)));
        }

        break;
      case ExpressionType.AndAlso:
        if (lhsFilter is TFilterOr lhsOrFilter2)
        {
          if (rhsFilter is TFilterOr rhsOrFilter)
          {
            // <or expression> && <or expression>
            var orFilter = new TFilterOr();
            foreach (var lhsAndFilter in GetRepeatedFilterAnd(lhsOrFilter2))
            {
              foreach (var rhsAndFilter in GetRepeatedFilterAnd(rhsOrFilter))
              {
                var andFilter = CreateFilterAnd();
                GetRepeatedFilterField(andFilter)
                  .Add(GetRepeatedFilterField(lhsAndFilter));
                GetRepeatedFilterField(andFilter)
                  .Add(GetRepeatedFilterField(rhsAndFilter));
                GetRepeatedFilterAnd(orFilter)
                  .Add(andFilter);
              }
            }

            FilterStack.Push((orFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <or expression> && <and expression>
            var orFilter = new TFilterOr();
            foreach (var lhsAnd in GetRepeatedFilterAnd(lhsOrFilter2))
            {
              var andFilter = new TFilterAnd();
              GetRepeatedFilterField(andFilter)
                .Add(GetRepeatedFilterField(lhsAnd));
              foreach (var rhsAnd in GetRepeatedFilterField(rhsAndFilter))
              {
                GetRepeatedFilterField(andFilter)
                  .Add(rhsAnd);
              }

              GetRepeatedFilterAnd(orFilter)
                .Add(andFilter);
            }

            FilterStack.Push((orFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <or expression> && <filter field>
            foreach (var and in GetRepeatedFilterAnd(lhsOrFilter2))
            {
              GetRepeatedFilterField(and)
                .Add(rhsFilterField);
            }

            FilterStack.Push((lhsOrFilter2, typeof(bool)));
          }
          else if (rhsFilter is bool rhsValue)
          {
            // <or expression> && <bool>
            if (rhsValue)
            {
              FilterStack.Push((lhsOrFilter2, typeof(bool)));
            }
            else
            {
              FilterStack.Push((false, typeof(bool)));
            }
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }
        else if (lhsFilter is TFilterAnd lhsAndFilter)
        {
          if (rhsFilter is TFilterOr rhsOrFilter)
          {
            // <and expression> && <or expression>
            var orFilter = new TFilterOr();
            foreach (var rhsAndFilter in GetRepeatedFilterAnd(rhsOrFilter))
            {
              var andFilter = new TFilterAnd();
              GetRepeatedFilterField(andFilter)
                .Add(GetRepeatedFilterField(lhsAndFilter));
              GetRepeatedFilterField(andFilter)
                .Add(GetRepeatedFilterField(rhsAndFilter));
              GetRepeatedFilterAnd(orFilter)
                .Add(andFilter);
            }

            FilterStack.Push((orFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterAnd rhsAndFilter)
          {
            // <and expression> && <and expression>
            foreach (var rhsFilterField in GetRepeatedFilterField(rhsAndFilter))
            {
              GetRepeatedFilterField(lhsAndFilter)
                .Add(rhsFilterField);
            }

            FilterStack.Push((lhsAndFilter, typeof(bool)));
          }
          else if (rhsFilter is TFilterField rhsFilterField)
          {
            // <and expression> && <filter field>
            GetRepeatedFilterField(lhsAndFilter)
              .Add(rhsFilterField);
            FilterStack.Push((lhsAndFilter, typeof(bool)));
          }
          else if (rhsFilter is bool rhsValue)
          {
            // <and expression> && <bool>
            if (rhsValue)
            {
              FilterStack.Push((lhsAndFilter, typeof(bool)));
            }
            else
            {
              FilterStack.Push((false, typeof(bool)));
            }
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }
        else if (lhsFilter is TFilterField lhsFilterField)
        {
          if (rhsFilter is TFilterField rhsFilterField)
          {
            // <filter field> && <filter field>
            var andFilter = CreateFilterAnd(lhsFilterField,
                                            rhsFilterField);
            FilterStack.Push((andFilter!, typeof(bool)));
          }
          else if (rhsFilter is bool rhsValue)
          {
            // <filter field> && <bool>
            if (rhsValue)
            {
              FilterStack.Push((lhsFilterField, typeof(bool)));
            }
            else
            {
              FilterStack.Push((false, typeof(bool)));
            }
          }
          else
          {
            throw new InvalidOperationException("Internal error: invalid boolean expression.");
          }
        }
        else if (lhsFilter is bool lhsValue)
        {
          // <bool> && <any>
          FilterStack.Push((lhsValue
                              ? rhsFilter
                              : false, typeof(bool)));
        }

        break;
      default:
        throw new InvalidOperationException($"Internal error: operator '{type}' is not supported on operands of type bool.");
    }
  }


  private void VisitContainsMethod(MethodCallExpression call,
                                   bool                 notOp)
  {
    var (containsParam, _) = FilterStack.Peek();
    if (containsParam is TField field)
    {
      // filter of the form : collection.Contains(field)
      FilterStack.Pop();                       // Pop the enum field
      var (collection, _) = FilterStack.Pop(); // Pop the collection

      OnCollectionContains(field,
                           ((IEnumerable)collection).Cast<object>(),
                           notOp);
    }
    else
    {
      throw new InvalidOperationException("Invalid filter: illegal use of method Contains");
    }
  }

  protected abstract void OnCollectionContains(TField              taskField,
                                               IEnumerable<object> collection,
                                               bool                notOp = false);

  protected TFilterField CreateFilterField(TField taskField,
                                           object val,
                                           bool   isEqual)
    => val switch
       {
         int number => CreateNumberFilter(taskField,
                                          isEqual
                                            ? FilterNumberOperator.Equal
                                            : FilterNumberOperator.NotEqual,
                                          number),
         string str => CreateStringFilter(taskField,
                                          isEqual
                                            ? FilterStringOperator.Equal
                                            : FilterStringOperator.NotEqual,
                                          str),
         char car => CreateStringFilter(taskField,
                                        isEqual
                                          ? FilterStringOperator.Equal
                                          : FilterStringOperator.NotEqual,
                                        car.ToString()),
         DateTime date => CreateDateFilter(taskField,
                                           isEqual
                                             ? FilterDateOperator.Equal
                                             : FilterDateOperator.NotEqual,
                                           date.ToUniversalTime()
                                               .ToTimestamp()),
         TimeSpan timeSpan => CreateDurationFilter(taskField,
                                                   isEqual
                                                     ? FilterDurationOperator.Equal
                                                     : FilterDurationOperator.NotEqual,
                                                   Duration.FromTimeSpan(timeSpan)),
         TaskStatus status => CreateTaskStatusFilter(taskField,
                                                     isEqual
                                                       ? FilterStatusOperator.Equal
                                                       : FilterStatusOperator.NotEqual,
                                                     status.ToGrpcStatus()),
         BlobStatus status => CreateBlobStatusFilter(taskField,
                                                     isEqual
                                                       ? FilterStatusOperator.Equal
                                                       : FilterStatusOperator.NotEqual,
                                                     status.ToGrpcStatus()),
         _ => throw new InvalidOperationException($"Unsupported constant type '{val.GetType()}' in filter expression."),
       };

  private void OnStringMethodOperator(MethodInfo method,
                                      bool       notOp = false)
  {
    switch (method.Name)
    {
      case nameof(string.StartsWith):
        PushStringFilter(FilterStringOperator.StartsWith);
        break;
      case nameof(string.EndsWith):
        PushStringFilter(FilterStringOperator.EndsWith);
        break;
      case nameof(string.Contains):
        PushStringFilter(notOp
                           ? FilterStringOperator.NotContains
                           : FilterStringOperator.Contains);
        break;
      default:
        throw new InvalidOperationException($"Method string.{method.Name} is not supported to filter tasks.");
    }
  }

  private void HandleStringExpression(ExpressionType type)
  {
    FilterStringOperator op;
    switch (type)
    {
      case ExpressionType.Equal:
        op = FilterStringOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        op = FilterStringOperator.NotEqual;
        break;
      default:
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type string.");
    }

    PushStringFilter(op);
  }

  private void PushStringFilter(FilterStringOperator op)
  {
    var fieldCount = 0;
    var constCount = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
    var     field = default(TField);
    string? val   = null;
    if (lhsFilter is TField lhsField)
    {
      // Left hand side is the property
      field = lhsField;
      fieldCount++;
    }
    else if (lhsFilter is string str)
    {
      // Left hand side is a constant
      val = str;
      constCount++;
    }
    else if (lhsFilter is char car)
    {
      // Left hand side is a constant
      val = car.ToString();
      constCount++;
    }

    if (rhsFilter is TField rhsField)
    {
      // Right hand side is the property
      field = rhsField;
      fieldCount++;
    }
    else if (rhsFilter is string str)
    {
      // Right hand side is a constant
      val = str;
      constCount++;
    }
    else if (rhsFilter is char car)
    {
      // Right hand side is a constant
      val = car.ToString();
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((CreateStringFilter(field!,
                                         op,
                                         val!)!, typeof(bool)));
  }

  private void HandleIntegerExpression(ExpressionType type)
  {
    FilterNumberOperator op;
    switch (type)
    {
      case ExpressionType.Equal:
        op = FilterNumberOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        op = FilterNumberOperator.NotEqual;
        break;
      case ExpressionType.LessThan:
        op = FilterNumberOperator.LessThan;
        break;
      case ExpressionType.LessThanOrEqual:
        op = FilterNumberOperator.LessThanOrEqual;
        break;
      case ExpressionType.GreaterThan:
        op = FilterNumberOperator.GreaterThan;
        break;
      case ExpressionType.GreaterThanOrEqual:
        op = FilterNumberOperator.GreaterThanOrEqual;
        break;
      default:
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type integer.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
    var  field = default(TField);
    int? val   = null;
    if (lhsFilter is TField lhsField)
    {
      // Left hand side is the property
      field = lhsField;
      fieldCount++;
    }
    else if (lhsFilter is int number)
    {
      // Left hand side is a constant
      val = number;
      constCount++;
    }

    if (rhsFilter is TField rhsField)
    {
      // Right hand side is the property
      field = rhsField;
      fieldCount++;
    }
    else if (rhsFilter is int number)
    {
      // Right hand side is a constant
      val = number;
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((CreateNumberFilter(field!,
                                         op,
                                         val!.Value)!, typeof(bool)));
  }

  private void HandleDateTimeExpression(ExpressionType type)
  {
    FilterDateOperator op;
    switch (type)
    {
      case ExpressionType.Equal:
        op = FilterDateOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        op = FilterDateOperator.NotEqual;
        break;
      case ExpressionType.LessThan:
        op = FilterDateOperator.Before;
        break;
      case ExpressionType.LessThanOrEqual:
        op = FilterDateOperator.BeforeOrEqual;
        break;
      case ExpressionType.GreaterThan:
        op = FilterDateOperator.After;
        break;
      case ExpressionType.GreaterThanOrEqual:
        op = FilterDateOperator.AfterOrEqual;
        break;
      default:
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type DateTime.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
    var        field = default(TField);
    Timestamp? val   = null;
    if (lhsFilter is TField lhsField)
    {
      // Left hand side is the property
      field = lhsField;
      fieldCount++;
    }
    else if (lhsFilter is DateTime date)
    {
      // Left hand side is a constant
      val = date.ToUniversalTime()
                .ToTimestamp();
      constCount++;
    }

    if (rhsFilter is TField rhsField)
    {
      // Right hand side is the property
      field = rhsField;
      fieldCount++;
    }
    else if (rhsFilter is DateTime date)
    {
      // Right hand side is a constant
      val = date.ToUniversalTime()
                .ToTimestamp();
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((CreateDateFilter(field!,
                                       op,
                                       val!)!, typeof(bool)));
  }

  private void HandleTimeSpanExpression(ExpressionType type)
  {
    FilterDurationOperator op;
    switch (type)
    {
      case ExpressionType.Equal:
        op = FilterDurationOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        op = FilterDurationOperator.NotEqual;
        break;
      case ExpressionType.GreaterThan:
        op = FilterDurationOperator.LongerThan;
        break;
      case ExpressionType.GreaterThanOrEqual:
        op = FilterDurationOperator.LongerThanOrEqual;
        break;
      case ExpressionType.LessThan:
        op = FilterDurationOperator.ShorterThan;
        break;
      case ExpressionType.LessThanOrEqual:
        op = FilterDurationOperator.ShorterThanOrEqual;
        break;
      default:
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type TimeSpan.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
    var       field = default(TField);
    Duration? val   = null;
    if (lhsFilter is TField lhsField)
    {
      // Left hand side is the property
      field = lhsField;
      fieldCount++;
    }
    else if (lhsFilter is TimeSpan timeSpan)
    {
      // Left hand side is a constant
      val = Duration.FromTimeSpan(timeSpan);
      constCount++;
    }

    if (rhsFilter is TField rhsField)
    {
      // Right hand side is the property
      field = rhsField;
      fieldCount++;
    }
    else if (rhsFilter is TimeSpan timeSpan)
    {
      // Right hand side is a constant
      val = Duration.FromTimeSpan(timeSpan);
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      // Invalid expression
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((CreateDurationFilter(field!,
                                           op,
                                           val!)!, typeof(bool)));
  }

  private void HandleTaskStatusExpression(ExpressionType type)
  {
    FilterStatusOperator op;
    switch (type)
    {
      case ExpressionType.Equal:
        op = FilterStatusOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        op = FilterStatusOperator.NotEqual;
        break;
      default:
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type BlobStatus.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
    var                     field = default(TField);
    Api.gRPC.V1.TaskStatus? val   = null;
    if (lhsFilter is TField lhsField)
    {
      // Left hand side is the property
      field = lhsField;
      fieldCount++;
    }
    else if (lhsFilter is TaskStatus status)
    {
      // Left hand side is a constant
      val = status.ToGrpcStatus();
      constCount++;
    }
    else if (lhsFilter is int statusInt)
    {
      // Left hand side is a constant
      val = ((TaskStatus)statusInt).ToGrpcStatus();
      constCount++;
    }

    if (rhsFilter is TField rhsField)
    {
      // Right hand side is the property
      field = rhsField;
      fieldCount++;
    }
    else if (rhsFilter is TaskStatus status)
    {
      // Right hand side is a constant
      val = status.ToGrpcStatus();
      constCount++;
    }
    else if (rhsFilter is int statusInt)
    {
      // Right hand side is a constant
      val = ((TaskStatus)statusInt).ToGrpcStatus();
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((CreateTaskStatusFilter(field!,
                                             op,
                                             val!.Value)!, typeof(bool)));
  }

  private void HandleBlobStatusExpression(ExpressionType type)
  {
    FilterStatusOperator op;
    switch (type)
    {
      case ExpressionType.Equal:
        op = FilterStatusOperator.Equal;
        break;
      case ExpressionType.NotEqual:
        op = FilterStatusOperator.NotEqual;
        break;
      default:
        throw new InvalidOperationException($"Operator '{type}' is not supported on operands of type BlobStatus.");
    }

    var fieldCount = 0;
    var constCount = 0;
    var (rhsFilter, _) = FilterStack.Pop();
    var (lhsFilter, _) = FilterStack.Pop();
    var           field = default(TField);
    ResultStatus? val   = null;
    if (lhsFilter is TField lhsFilterField)
    {
      // Left hand side is the property
      field = lhsFilterField;
      fieldCount++;
    }
    else if (lhsFilter is BlobStatus status)
    {
      // Left hand side is a constant
      val = status.ToGrpcStatus();
      constCount++;
    }
    else if (lhsFilter is int statusInt)
    {
      // Left hand side is a constant
      val = ((BlobStatus)statusInt).ToGrpcStatus();
      constCount++;
    }

    if (rhsFilter is TField rhsFilterField)
    {
      // Right hand side is the property
      field = rhsFilterField;
      fieldCount++;
    }
    else if (rhsFilter is BlobStatus status)
    {
      // Right hand side is a constant
      val = status.ToGrpcStatus();
      constCount++;
    }
    else if (rhsFilter is int statusInt)
    {
      // Right hand side is a constant
      val = ((BlobStatus)statusInt).ToGrpcStatus();
      constCount++;
    }

    if (fieldCount != 1 || constCount != 1)
    {
      throw new InvalidOperationException("Invalid filter expression.");
    }

    FilterStack.Push((CreateBlobStatusFilter(field!,
                                             op,
                                             val!.Value)!, typeof(bool)));
  }


  protected abstract TFilterField CreateStringFilter(TField               field,
                                                     FilterStringOperator op,
                                                     string               val);

  protected abstract TFilterField CreateNumberFilter(TField               field,
                                                     FilterNumberOperator op,
                                                     int                  val);

  protected abstract TFilterField CreateDateFilter(TField             field,
                                                   FilterDateOperator op,
                                                   Timestamp          val);

  protected abstract TFilterField CreateDurationFilter(TField                 field,
                                                       FilterDurationOperator op,
                                                       Duration               val);

  protected abstract TFilterField CreateTaskStatusFilter(TField                 field,
                                                         FilterStatusOperator   op,
                                                         Api.gRPC.V1.TaskStatus val);

  protected abstract TFilterField CreateBlobStatusFilter(TField               field,
                                                         FilterStatusOperator op,
                                                         ResultStatus         val);

  protected abstract void OnIndexerAccess();
  protected abstract bool PushProperty(MemberExpression member);
}
