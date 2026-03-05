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

using ArmoniK.Api.gRPC.V1;

namespace Tests.Queryable;

internal class FiltersMaps
{
  public static readonly Dictionary<string, FilterStringOperator> Op2EnumStringOp_ = new()
                                                                                     {
                                                                                       {
                                                                                         "==", FilterStringOperator.Equal
                                                                                       },
                                                                                       {
                                                                                         "!=", FilterStringOperator.NotEqual
                                                                                       },
                                                                                       {
                                                                                         "Contains", FilterStringOperator.Contains
                                                                                       },
                                                                                       {
                                                                                         "NotContains", FilterStringOperator.NotContains
                                                                                       },
                                                                                       {
                                                                                         "StartsWith", FilterStringOperator.StartsWith
                                                                                       },
                                                                                       {
                                                                                         "EndsWith", FilterStringOperator.EndsWith
                                                                                       },
                                                                                     };

  public static readonly Dictionary<string, FilterNumberOperator> Op2EnumIntOp_ = new()
                                                                                  {
                                                                                    {
                                                                                      "==", FilterNumberOperator.Equal
                                                                                    },
                                                                                    {
                                                                                      "!=", FilterNumberOperator.NotEqual
                                                                                    },
                                                                                    {
                                                                                      "<", FilterNumberOperator.LessThan
                                                                                    },
                                                                                    {
                                                                                      "<=", FilterNumberOperator.LessThanOrEqual
                                                                                    },
                                                                                    {
                                                                                      ">", FilterNumberOperator.GreaterThan
                                                                                    },
                                                                                    {
                                                                                      ">=", FilterNumberOperator.GreaterThanOrEqual
                                                                                    },
                                                                                  };

  public static readonly Dictionary<string, FilterStatusOperator> Op2EnumStatusOp_ = new()
                                                                                     {
                                                                                       {
                                                                                         "==", FilterStatusOperator.Equal
                                                                                       },
                                                                                       {
                                                                                         "!=", FilterStatusOperator.NotEqual
                                                                                       },
                                                                                     };

  public static readonly Dictionary<string, FilterDateOperator> Op2EnumDateOp_ = new()
                                                                                 {
                                                                                   {
                                                                                     "==", FilterDateOperator.Equal
                                                                                   },
                                                                                   {
                                                                                     "!=", FilterDateOperator.NotEqual
                                                                                   },
                                                                                   {
                                                                                     "<", FilterDateOperator.Before
                                                                                   },
                                                                                   {
                                                                                     "<=", FilterDateOperator.BeforeOrEqual
                                                                                   },
                                                                                   {
                                                                                     ">", FilterDateOperator.After
                                                                                   },
                                                                                   {
                                                                                     ">=", FilterDateOperator.AfterOrEqual
                                                                                   },
                                                                                 };

  public static readonly Dictionary<string, FilterDurationOperator> Op2EnumDurationOp_ = new()
                                                                                         {
                                                                                           {
                                                                                             "==", FilterDurationOperator.Equal
                                                                                           },
                                                                                           {
                                                                                             "!=", FilterDurationOperator.NotEqual
                                                                                           },
                                                                                           {
                                                                                             "<", FilterDurationOperator.ShorterThan
                                                                                           },
                                                                                           {
                                                                                             "<=", FilterDurationOperator.ShorterThanOrEqual
                                                                                           },
                                                                                           {
                                                                                             ">", FilterDurationOperator.LongerThan
                                                                                           },
                                                                                           {
                                                                                             ">=", FilterDurationOperator.LongerThanOrEqual
                                                                                           },
                                                                                         };

  public static readonly Dictionary<string, FilterArrayOperator> Op2EnumArrayOp_ = new()
                                                                                   {
                                                                                     {
                                                                                       "Contains", FilterArrayOperator.Contains
                                                                                     },
                                                                                     {
                                                                                       "NotContains", FilterArrayOperator.NotContains
                                                                                     },
                                                                                   };
}
