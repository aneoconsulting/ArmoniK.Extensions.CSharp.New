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

using ArmoniK.Extensions.CSharp.Client.Common;

using Microsoft.Extensions.Configuration;

using NUnit.Framework;

namespace Tests.Common;

[TestFixture]
public class ClientServiceConnectorTests
{
  private static Properties BuildProperties(Dictionary<string, string?> settings)
  {
    settings["Grpc:EndPoint"] = "http://localhost:5001";
    var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings)
                                                  .Build();
    return new Properties(configuration);
  }

  [Test]
  public void CreateGrpcClientOptions_UsesDefaultRetrySettings()
  {
    var options = ClientServiceConnector.CreateGrpcClientOptions(BuildProperties(new Dictionary<string, string?>()));

    Assert.Multiple(() =>
                    {
                      Assert.That(options.InitialBackOff,
                                  Is.EqualTo(TimeSpan.FromSeconds(1)));
                      Assert.That(options.BackoffMultiplier,
                                  Is.EqualTo(2));
                      Assert.That(options.MaxBackOff,
                                  Is.EqualTo(TimeSpan.FromSeconds(30)));
                    });
  }

  [Test]
  public void CreateGrpcClientOptions_AppliesConfiguredRetrySettings()
  {
    var options = ClientServiceConnector.CreateGrpcClientOptions(BuildProperties(new Dictionary<string, string?>
                                                                                 {
                                                                                   ["Grpc:RetryInitialBackoff"]    = "00:00:03",
                                                                                   ["Grpc:RetryBackoffMultiplier"] = "1.5",
                                                                                   ["Grpc:RetryMaxBackoff"]        = "00:00:12",
                                                                                 }));

    Assert.Multiple(() =>
                    {
                      Assert.That(options.InitialBackOff,
                                  Is.EqualTo(TimeSpan.FromSeconds(3)));
                      Assert.That(options.BackoffMultiplier,
                                  Is.EqualTo(1.5));
                      Assert.That(options.MaxBackOff,
                                  Is.EqualTo(TimeSpan.FromSeconds(12)));
                    });
  }
}
