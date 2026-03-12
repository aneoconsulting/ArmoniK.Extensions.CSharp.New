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

using System.Diagnostics.CodeAnalysis;

using ArmoniK.Extensions.CSharp.Client.Queryable;
using ArmoniK.Extensions.CSharp.Common.Common.Domain.Blob;

namespace ArmoniK.EndToEndTests.Client.Tests;

internal class FiltersTestClient : ClientBase
{
  [SetUp]
  public async Task SetupAsync()
  {
    await SetupBaseWithoutLibAsync()
      .ConfigureAwait(false);

    // Create 100 blobs with names in the range 1..100
    await Client!.BlobService.CreateBlobsMetadataAsync(SessionHandle!,
                                                       Enumerable.Range(1,
                                                                        100)
                                                                 .Select(i => ($"{i,3}", false)))
                 .ToArrayAsync()
                 .ConfigureAwait(false);
  }

  [TearDown]
  public async Task TearDownAsync()
    => await TearDownBaseAsync()
         .ConfigureAwait(false);

  [Test]
  public async Task SkipTakeTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;
    var skipTake = await Client!.BlobService.AsQueryable()
                                .Where(b => b.SessionId == sessionId)
                                .Skip(5)
                                .Take(3)
                                .OrderBy(b => b.BlobName)
                                .AsAsyncEnumerable()
                                .Select(b => b.BlobName.TrimStart())
                                .ToArrayAsync()
                                .ConfigureAwait(false);
    var takeSkip = await Client!.BlobService.AsQueryable()
                                .Where(b => b.SessionId == sessionId)
                                .Take(8)
                                .Skip(5)
                                .OrderBy(b => b.BlobName)
                                .AsAsyncEnumerable()
                                .Select(b => b.BlobName.TrimStart())
                                .ToArrayAsync()
                                .ConfigureAwait(false);

    string[] result = ["6", "7", "8"];
    Assert.Multiple(() =>
                    {
                      Assert.That(skipTake,
                                  Is.EquivalentTo(result));
                      Assert.That(takeSkip,
                                  Is.EquivalentTo(result));
                    });
  }

  [Test]
  public async Task TakeWhileTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;
    // Take the elements in increasing order while the element contains 2 whitespaces
    var takeWhile1 = await Client!.BlobService.AsQueryable()
                                  .Where(b => b.SessionId == sessionId)
                                  .TakeWhile(b => b.BlobName.Contains("  "))
                                  .OrderBy(b => b.BlobName)
                                  .AsAsyncEnumerable()
                                  .Select(b => b.BlobName.TrimStart())
                                  .ToArrayAsync()
                                  .ConfigureAwait(false);
    // Take the elements in increasing order while the index <= 3
    var takeWhile2 = await Client!.BlobService.AsQueryable()
                                  .Where(b => b.SessionId == sessionId)
                                  .TakeWhile((b,
                                              index) => index < 3)
                                  .OrderBy(b => b.BlobName)
                                  .AsAsyncEnumerable()
                                  .Select(b => b.BlobName.TrimStart())
                                  .ToArrayAsync()
                                  .ConfigureAwait(false);

    string[] resultTakeWhile1 = ["1", "2", "3", "4", "5", "6", "7", "8", "9"];
    string[] resultTakeWhile2 = ["1", "2", "3"];
    Assert.Multiple(() =>
                    {
                      Assert.That(takeWhile1,
                                  Is.EquivalentTo(resultTakeWhile1));
                      Assert.That(takeWhile2,
                                  Is.EquivalentTo(resultTakeWhile2));
                    });
  }

  [Test]
  public async Task SkipWhileTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;
    // Skip the elements in decreasing order while the element does not contain 2 whitespaces
    var skipWhile1 = await Client!.BlobService.AsQueryable()
                                  .SkipWhile(b => !b.BlobName.Contains("  "))
                                  .Where(b => b.SessionId == sessionId)
                                  .OrderByDescending(b => b.BlobName)
                                  .AsAsyncEnumerable()
                                  .Select(b => b.BlobName.TrimStart())
                                  .ToArrayAsync()
                                  .ConfigureAwait(false);
    // Skip the elements in increasing order while the element contains whitespaces
    var skipWhile2 = await Client!.BlobService.AsQueryable()
                                  .SkipWhile((b,
                                              index) => b.BlobName.Contains(" "))
                                  .Where(b => b.SessionId == sessionId)
                                  .OrderBy(b => b.BlobName)
                                  .AsAsyncEnumerable()
                                  .Select(b => b.BlobName.TrimStart())
                                  .ToArrayAsync()
                                  .ConfigureAwait(false);

    string[] resultSkipWhile1 = ["9", "8", "7", "6", "5", "4", "3", "2", "1"];
    string[] resultSkipWhile2 = ["100"];
    Assert.Multiple(() =>
                    {
                      Assert.That(skipWhile1,
                                  Is.EquivalentTo(resultSkipWhile1));
                      Assert.That(skipWhile2,
                                  Is.EquivalentTo(resultSkipWhile2));
                    });
  }

  [Test]
  public async Task DistinctTest()
  {
    // Create 10 duplicates
    await Client!.BlobService.CreateBlobsMetadataAsync(SessionHandle!,
                                                       Enumerable.Range(1,
                                                                        10)
                                                                 .Select(i => ($"{i,3}", false)))
                 .ToArrayAsync()
                 .ConfigureAwait(false);
    var sessionId    = SessionHandle!.SessionInfo.SessionId;
    var blobComparer = new BlobStateComparer();

    // Take the elements in increasing order while the element contains 2 whitespaces
    var distinct1 = await Client!.BlobService.AsQueryable()
                                 .Where(b => b.SessionId == sessionId)
                                 .Distinct()
                                 .Take(10)
                                 .OrderBy(b => b.BlobName)
                                 .AsAsyncEnumerable()
                                 .Select(b => b.BlobName.TrimStart())
                                 .ToArrayAsync()
                                 .ConfigureAwait(false);
    // Take the elements in increasing order while the whitespace count >= the element index
    var distinct2 = await Client!.BlobService.AsQueryable()
                                 .Where(b => b.SessionId == sessionId)
                                 .Distinct(blobComparer)
                                 .Take(10)
                                 .OrderBy(b => b.BlobName)
                                 .AsAsyncEnumerable()
                                 .Select(b => b.BlobName.TrimStart())
                                 .ToArrayAsync()
                                 .ConfigureAwait(false);

    string[] resultDistinct1 = ["1", "1", "2", "2", "3", "3", "4", "4", "5", "5"];
    string[] resultDistinct2 = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];
    Assert.Multiple(() =>
                    {
                      Assert.That(distinct1,
                                  Is.EquivalentTo(resultDistinct1));
                      Assert.That(distinct2,
                                  Is.EquivalentTo(resultDistinct2));
                    });
  }

  internal class BlobStateComparer : IEqualityComparer<BlobState>
  {
    public bool Equals(BlobState? x,
                       BlobState? y)
    {
      if (x == null || y == null)
      {
        return false;
      }

      return x.BlobName == y.BlobName;
    }

    public int GetHashCode([DisallowNull] BlobState obj)
      => obj.BlobName.GetHashCode();
  }
}
