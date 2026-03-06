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
using System.Linq.Expressions;

using ArmoniK.Api.gRPC.V1;
using ArmoniK.Api.gRPC.V1.Results;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Blob;

using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;

using Type = System.Type;

namespace ArmoniK.Extensions.CSharp.Client.Queryable.BlobStateQuery;

/// <summary>
///   Specialisation of WhereExpressionTreeVisitor for queries on BlobState instances.
/// </summary>
internal class BlobStateWhereExpressionTreeVisitor : WhereExpressionTreeVisitor<ResultField, Filters, FiltersAnd, FilterField>
{
  private static readonly Dictionary<string, Type> MemberName2Type_ = new()
                                                                      {
                                                                        {
                                                                          nameof(BlobInfo.SessionId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(BlobInfo.BlobId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(BlobInfo.BlobName), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(BlobInfo.CreatedBy), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(BlobState.CompletedAt), typeof(DateTime?)
                                                                        },
                                                                        {
                                                                          nameof(BlobState.CreateAt), typeof(DateTime)
                                                                        },
                                                                        {
                                                                          nameof(BlobState.Status), typeof(BlobStatus)
                                                                        },
                                                                        {
                                                                          nameof(BlobState.OwnerId), typeof(string)
                                                                        },
                                                                        {
                                                                          nameof(BlobState.OpaqueId), typeof(byte[])
                                                                        },
                                                                        {
                                                                          nameof(BlobState.Size), typeof(int)
                                                                        },
                                                                      };

  private static readonly Dictionary<string, ResultRawEnumField> MemberName2EnumField_ = new()
                                                                                         {
                                                                                           {
                                                                                             nameof(BlobInfo.SessionId), ResultRawEnumField.SessionId
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobInfo.BlobId), ResultRawEnumField.ResultId
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobInfo.BlobName), ResultRawEnumField.Name
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobInfo.CreatedBy), ResultRawEnumField.CreatedBy
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.CompletedAt), ResultRawEnumField.CompletedAt
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.CreateAt), ResultRawEnumField.CreatedAt
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.Status), ResultRawEnumField.Status
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.OwnerId), ResultRawEnumField.OwnerTaskId
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.OpaqueId), ResultRawEnumField.OpaqueId
                                                                                           },
                                                                                           {
                                                                                             nameof(BlobState.Size), ResultRawEnumField.Size
                                                                                           },
                                                                                         };

  protected override Filters CreateFilterOr(params FiltersAnd[] filters)
  {
    var orFilter = new Filters();
    orFilter.Or.Add(filters);
    return orFilter;
  }

  protected override FiltersAnd CreateFilterAnd(params FilterField[] filters)
  {
    var andFilter = new FiltersAnd();
    andFilter.And.Add(filters);
    return andFilter;
  }

  protected override RepeatedField<FiltersAnd> GetRepeatedFilterAnd(Filters or)
    => or.Or;

  protected override RepeatedField<FilterField> GetRepeatedFilterField(FiltersAnd and)
    => and.And;

  protected override FilterField CreateStringFilter(ResultField          field,
                                                    FilterStringOperator op,
                                                    string               val)
  {
    var filterField = new FilterField();
    filterField.FilterString = new FilterString
                               {
                                 Operator = op,
                                 Value    = val,
                               };
    filterField.Field = field;
    return filterField;
  }

  protected override FilterField CreateNumberFilter(ResultField          field,
                                                    FilterNumberOperator op,
                                                    long                 val)
  {
    var filterField = new FilterField();
    filterField.FilterNumber = new FilterNumber
                               {
                                 Operator = op,
                                 Value    = val,
                               };
    filterField.Field = field;
    return filterField;
  }

  protected override FilterField CreateDateFilter(ResultField        field,
                                                  FilterDateOperator op,
                                                  Timestamp          val)
  {
    var filterField = new FilterField();
    filterField.FilterDate = new FilterDate
                             {
                               Operator = op,
                               Value    = val,
                             };
    filterField.Field = field;
    return filterField;
  }

  protected override FilterField CreateDurationFilter(ResultField            field,
                                                      FilterDurationOperator op,
                                                      Duration               val)
    // should never happen since there is no Duration property in BlobState
    => throw new InvalidOperationException("Invalid filter: Duration filter is not supported for BlobState queries.");

  protected override FilterField CreateTaskStatusFilter(ResultField          field,
                                                        FilterStatusOperator op,
                                                        TaskStatus           val)
    // should never happen since there is no TaskStatus property in BlobState
    => throw new InvalidOperationException("Invalid filter: TaskStatus filter is not supported for BlobState queries.");

  protected override FilterField CreateBlobStatusFilter(ResultField          field,
                                                        FilterStatusOperator op,
                                                        ResultStatus         val)
  {
    var filterField = new FilterField();
    filterField.FilterStatus = new FilterStatus
                               {
                                 Operator = op,
                                 Value    = val,
                               };
    filterField.Field = field;
    return filterField;
  }

  protected override FilterField CreateArrayFilter(ResultField         field,
                                                   FilterArrayOperator op,
                                                   string              val)
    // should never happen since there is no criteria to filter on an array property in BlobState
    => throw new InvalidOperationException("Invalid filter: array filters are not supported for BlobState queries.");

  protected override void OnIndexerAccess()
    // That case never happen for BlobState instances and is not supported
    => throw new InvalidOperationException("Invalid filter expression.");

  protected override bool PushProperty(MemberExpression member)
  {
    if (MemberName2Type_.TryGetValue(member.Member.Name,
                                     out var memberType) && MemberName2EnumField_.TryGetValue(member.Member.Name,
                                                                                              out var enumField))
    {
      var field = new ResultField
                  {
                    ResultRawField = new ResultRawField
                                     {
                                       Field = enumField,
                                     },
                  };
      FilterStack.Push((field, memberType));
      return true;
    }

    return false;
  }

  protected override void OnCollectionContains(ResultField         field,
                                               IEnumerable<object> collection,
                                               bool                notOp = false)
  {
    var isEmpty = true;
    var orNode  = new Filters();
    if (notOp)
    {
      orNode.Or.Add(new FiltersAnd());
    }

    foreach (var val in collection)
    {
      isEmpty = false;
      if (notOp)
      {
        orNode.Or[0]
              .And.Add(CreateFilterField(field,
                                         val,
                                         false));
      }
      else
      {
        orNode.Or.Add(new FiltersAnd
                      {
                        And =
                        {
                          CreateFilterField(field,
                                            val,
                                            true),
                        },
                      });
      }
    }

    if (isEmpty)
    {
      FilterStack.Push((notOp, typeof(bool)));
    }
    else
    {
      FilterStack.Push((orNode, typeof(bool)));
    }
  }
}
