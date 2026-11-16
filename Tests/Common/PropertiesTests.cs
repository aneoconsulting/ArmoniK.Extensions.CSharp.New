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

using ArmoniK.Extensions.CSharp.Client.Common;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

namespace Tests.Common;

[TestFixture]
public class PropertiesTests
{
  private static IConfiguration BuildConfiguration(Dictionary<string, string?> settings)
  {
    settings["Grpc:EndPoint"] = "http://localhost:5001";
    return new ConfigurationBuilder().AddInMemoryCollection(settings)
                                     .Build();
  }

  [Test]
  public void RetryMaxAttempts_DefaultsToFive()
    => Assert.That(new Properties(BuildConfiguration(new Dictionary<string, string?>())).RetryMaxAttempts,
                   Is.EqualTo(5));

  [Test]
  public void RetryMaxAttempts_IsReadFromConfiguration()
    => Assert.That(new Properties(BuildConfiguration(new Dictionary<string, string?>
                                                     {
                                                       ["Grpc:RetryMaxAttempts"] = "3",
                                                     })).RetryMaxAttempts,
                   Is.EqualTo(3));

  [Test]
  public void RetryMaxAttempts_ArgumentOverridesConfiguration()
    => Assert.That(new Properties(BuildConfiguration(new Dictionary<string, string?>
                                                     {
                                                       ["Grpc:RetryMaxAttempts"] = "3",
                                                     }),
                                  retryMaxAttempts: 4).RetryMaxAttempts,
                   Is.EqualTo(4));

  [TestCase("1")]
  [TestCase("0")]
  [TestCase("-2")]
  public void RetryMaxAttempts_ThrowsWhenLowerThanTwo(string value)
    => Assert.That(() => new Properties(BuildConfiguration(new Dictionary<string, string?>
                                                           {
                                                             ["Grpc:RetryMaxAttempts"] = value,
                                                           })),
                   Throws.InstanceOf<ArgumentOutOfRangeException>());
}
