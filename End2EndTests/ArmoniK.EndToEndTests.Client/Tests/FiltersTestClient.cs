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

  [Test]
  public void FirstTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;

    // First() without predicate in ascending order
    var first = Client!.BlobService.AsQueryable()
                       .Where(b => b.SessionId == sessionId)
                       .OrderBy(b => b.BlobName)
                       .First();

    // FirstOrDefault() without predicate in ascending order
    var firstOrDefault = Client!.BlobService.AsQueryable()
                                .Where(b => b.SessionId == sessionId)
                                .OrderBy(b => b.BlobName)
                                .FirstOrDefault();

    // First(predicate) - first blob with an exact name
    var firstWithPredicate = Client!.BlobService.AsQueryable()
                                    .Where(b => b.SessionId == sessionId)
                                    .OrderBy(b => b.BlobName)
                                    .First(b => b.BlobName == " 50");

    // FirstOrDefault(predicate) with no match - should return null
    var firstOrDefaultNoMatch = Client!.BlobService.AsQueryable()
                                       .Where(b => b.SessionId         == sessionId)
                                       .FirstOrDefault(b => b.BlobName == "xyz");

    Assert.Multiple(() =>
                    {
                      Assert.That(first.BlobName.TrimStart(),
                                  Is.EqualTo("1"));
                      Assert.That(firstOrDefault!.BlobName.TrimStart(),
                                  Is.EqualTo("1"));
                      Assert.That(firstWithPredicate.BlobName.TrimStart(),
                                  Is.EqualTo("50"));
                      Assert.That(firstOrDefaultNoMatch,
                                  Is.Null);
                    });
  }

  [Test]
  public void LastTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;

    // Last() without predicate in ascending order
    var last = Client!.BlobService.AsQueryable()
                      .Where(b => b.SessionId == sessionId)
                      .OrderBy(b => b.BlobName)
                      .Last();

    // LastOrDefault() without predicate in ascending order
    var lastOrDefault = Client!.BlobService.AsQueryable()
                               .Where(b => b.SessionId == sessionId)
                               .OrderBy(b => b.BlobName)
                               .LastOrDefault();

    // Last(predicate) - last blob with double-space prefix (names "  1".."  9")
    var lastWithPredicate = Client!.BlobService.AsQueryable()
                                   .Where(b => b.SessionId == sessionId)
                                   .OrderBy(b => b.BlobName)
                                   .Last(b => b.BlobName == "  9");

    // LastOrDefault(predicate) with no match - should return null
    var lastOrDefaultNoMatch = Client!.BlobService.AsQueryable()
                                      .Where(b => b.SessionId        == sessionId)
                                      .LastOrDefault(b => b.BlobName == "xyz");

    Assert.Multiple(() =>
                    {
                      Assert.That(last.BlobName.TrimStart(),
                                  Is.EqualTo("100"));
                      Assert.That(lastOrDefault!.BlobName.TrimStart(),
                                  Is.EqualTo("100"));
                      Assert.That(lastWithPredicate.BlobName.TrimStart(),
                                  Is.EqualTo("9"));
                      Assert.That(lastOrDefaultNoMatch,
                                  Is.Null);
                    });
  }

  [Test]
  public void SingleTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;

    // Single(predicate) - exactly one blob has name "100"
    var single = Client!.BlobService.AsQueryable()
                        .Where(b => b.SessionId == sessionId)
                        .Single(b => b.BlobName == "100");

    // SingleOrDefault(predicate) with one match
    var singleOrDefault = Client!.BlobService.AsQueryable()
                                 .Where(b => b.SessionId          == sessionId)
                                 .SingleOrDefault(b => b.BlobName == "100");

    // SingleOrDefault(predicate) with no match - should return null
    var singleOrDefaultNoMatch = Client!.BlobService.AsQueryable()
                                        .Where(b => b.SessionId          == sessionId)
                                        .SingleOrDefault(b => b.BlobName == "xyz");

    Assert.Multiple(() =>
                    {
                      Assert.That(single.BlobName,
                                  Is.EqualTo("100"));
                      Assert.That(singleOrDefault!.BlobName,
                                  Is.EqualTo("100"));
                      Assert.That(singleOrDefaultNoMatch,
                                  Is.Null);
                    });
  }

  [Test]
  public void AnyTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;

    // Any() without predicate - 100 blobs exist in the session
    var anyBlob = Client!.BlobService.AsQueryable()
                         .Where(b => b.SessionId == sessionId)
                         .Any();

    // Any(predicate) with a matching predicate
    var anyWithMatch = Client!.BlobService.AsQueryable()
                              .Where(b => b.SessionId == sessionId)
                              .Any(b => b.BlobName    == "100");

    // Any(predicate) with no match
    var anyNoMatch = Client!.BlobService.AsQueryable()
                            .Where(b => b.SessionId == sessionId)
                            .Any(b => b.BlobName    == "xyz");

    Assert.Multiple(() =>
                    {
                      Assert.That(anyBlob,
                                  Is.True);
                      Assert.That(anyWithMatch,
                                  Is.True);
                      Assert.That(anyNoMatch,
                                  Is.False);
                    });
  }

  [Test]
  public void AllTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;

    // All blobs in the session have a non-empty name
    var allHaveName = Client!.BlobService.AsQueryable()
                             .Where(b => b.SessionId == sessionId)
                             .All(b => b.BlobName    != "");

    // Not all blobs have name "100"
    var allNamed100 = Client!.BlobService.AsQueryable()
                             .Where(b => b.SessionId == sessionId)
                             .All(b => b.BlobName    == "100");

    Assert.Multiple(() =>
                    {
                      Assert.That(allHaveName,
                                  Is.True);
                      Assert.That(allNamed100,
                                  Is.False);
                    });
  }

  [Test]
  public void ContainsTest()
  {
    var sessionId    = SessionHandle!.SessionInfo.SessionId;
    var blobComparer = new BlobStateComparer();

    // Fetch a known blob to use as the search item
    var blob100 = Client!.BlobService.AsQueryable()
                         .Where(b => b.SessionId == sessionId)
                         .Single(b => b.BlobName == "100");

    // Contains with the exact blob fetched from the queryable - should return true
    var containsExact = Client!.BlobService.AsQueryable()
                               .Where(b => b.SessionId == sessionId)
                               .Contains(blob100);

    // Contains with a modified blob (different BlobId) - full equality fails, should return false
    var fakeBlob = blob100 with
                   {
                     BlobId = "fake-id",
                   };
    var doesNotContainFake = Client!.BlobService.AsQueryable()
                                    .Where(b => b.SessionId == sessionId)
                                    .Contains(fakeBlob);

    // Contains with BlobStateComparer (compares by name) - same name, different BlobId: should return true
    var containsWithComparer = Client!.BlobService.AsQueryable()
                                      .Where(b => b.SessionId == sessionId)
                                      .Contains(fakeBlob,
                                                blobComparer);

    Assert.Multiple(() =>
                    {
                      Assert.That(containsExact,
                                  Is.True);
                      Assert.That(doesNotContainFake,
                                  Is.False);
                      Assert.That(containsWithComparer,
                                  Is.True);
                    });
  }

  [Test]
  public async Task UnionTest()
  {
    var sessionId    = SessionHandle!.SessionInfo.SessionId;
    var blobComparer = new BlobStateComparer();

    // Fetch blobs 6-10 separately to reuse as source2
    var blob6To10 = await Client!.BlobService.AsQueryable()
                                 .Where(b => b.SessionId == sessionId)
                                 .OrderBy(b => b.BlobName)
                                 .Skip(5)
                                 .Take(5)
                                 .AsAsyncEnumerable()
                                 .ToArrayAsync()
                                 .ConfigureAwait(false);

    // Union blobs 6-10 with themselves: duplicates removed by full equality -> 5 blobs
    var union = await Client!.BlobService.AsQueryable()
                             .Where(b => b.SessionId == sessionId)
                             .OrderBy(b => b.BlobName)
                             .Skip(5)
                             .Take(5)
                             .Union(blob6To10)
                             .AsAsyncEnumerable()
                             .Select(b => b.BlobName.TrimStart())
                             .ToArrayAsync()
                             .ConfigureAwait(false);

    // Fetch blobs 4-10 to create an overlap with blobs 1-5
    var blob4To10 = await Client!.BlobService.AsQueryable()
                                 .Where(b => b.SessionId == sessionId)
                                 .OrderBy(b => b.BlobName)
                                 .Skip(3)
                                 .Take(7)
                                 .AsAsyncEnumerable()
                                 .ToArrayAsync()
                                 .ConfigureAwait(false);

    // Union blobs 1-5 with blobs 4-10 using name comparer: blobs 4 and 5 overlap -> 10 unique blobs
    var unionWithComparer = await Client!.BlobService.AsQueryable()
                                         .Where(b => b.SessionId == sessionId)
                                         .OrderBy(b => b.BlobName)
                                         .Take(5)
                                         .Union(blob4To10,
                                                blobComparer)
                                         .AsAsyncEnumerable()
                                         .Select(b => b.BlobName.TrimStart())
                                         .ToArrayAsync()
                                         .ConfigureAwait(false);

    string[] expectedUnion             = ["6", "7", "8", "9", "10"];
    string[] expectedUnionWithComparer = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"];
    Assert.Multiple(() =>
                    {
                      Assert.That(union,
                                  Is.EquivalentTo(expectedUnion));
                      Assert.That(unionWithComparer,
                                  Is.EquivalentTo(expectedUnionWithComparer));
                    });
  }

  [Test]
  public async Task SequenceEqualTest()
  {
    var sessionId    = SessionHandle!.SessionInfo.SessionId;
    var blobComparer = new BlobStateComparer();

    // Pre-fetch the first 3 blobs sorted ascending
    var first3 = await Client!.BlobService.AsQueryable()
                              .Where(b => b.SessionId == sessionId)
                              .OrderBy(b => b.BlobName)
                              .Take(3)
                              .AsAsyncEnumerable()
                              .ToArrayAsync()
                              .ConfigureAwait(false);

    // SequenceEqual with the same elements in the same order - should return true
    var equalsSameOrder = Client!.BlobService.AsQueryable()
                                 .Where(b => b.SessionId == sessionId)
                                 .OrderBy(b => b.BlobName)
                                 .Take(3)
                                 .SequenceEqual(first3);

    // SequenceEqual with elements in reversed order - should return false
    var equalsDifferentOrder = Client!.BlobService.AsQueryable()
                                      .Where(b => b.SessionId == sessionId)
                                      .OrderBy(b => b.BlobName)
                                      .Take(3)
                                      .SequenceEqual(first3.Reverse());

    // SequenceEqual with name comparer and same-named objects - should return true
    var equalsWithComparer = Client!.BlobService.AsQueryable()
                                    .Where(b => b.SessionId == sessionId)
                                    .OrderBy(b => b.BlobName)
                                    .Take(3)
                                    .SequenceEqual(first3,
                                                   blobComparer);

    Assert.Multiple(() =>
                    {
                      Assert.That(equalsSameOrder,
                                  Is.True);
                      Assert.That(equalsDifferentOrder,
                                  Is.False);
                      Assert.That(equalsWithComparer,
                                  Is.True);
                    });
  }

  [Test]
  public void ElementAtTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;

    // ElementAt(0) ascending: the first blob is "  1"
    var first = Client!.BlobService.AsQueryable()
                       .Where(b => b.SessionId == sessionId)
                       .OrderBy(b => b.BlobName)
                       .ElementAt(0);

    // ElementAt(4) ascending: the 5th blob is "  5"
    var fifth = Client!.BlobService.AsQueryable()
                       .Where(b => b.SessionId == sessionId)
                       .OrderBy(b => b.BlobName)
                       .ElementAt(4);

    // ElementAtOrDefault(200) with only 100 blobs: out of range, should return null
    var outOfRange = Client!.BlobService.AsQueryable()
                            .Where(b => b.SessionId == sessionId)
                            .ElementAtOrDefault(200);

    Assert.Multiple(() =>
                    {
                      Assert.That(first.BlobName.TrimStart(),
                                  Is.EqualTo("1"));
                      Assert.That(fifth.BlobName.TrimStart(),
                                  Is.EqualTo("5"));
                      Assert.That(outOfRange,
                                  Is.Null);
                    });
  }

  [Test]
  public void AggregateTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;

    // Among "  9", " 10" , "100", keep the blob with the longest trimmed name -> "100"
    var aggregate = Client!.BlobService.AsQueryable()
                           .Where(b => b.SessionId == sessionId && (b.BlobName == "  9" || b.BlobName == " 10" || b.BlobName == "100"))
                           .OrderBy(b => b.BlobName)
                           .Aggregate((acc,
                                       b) => b.BlobName.TrimStart()
                                              .Length > acc.BlobName.TrimStart()
                                                           .Length
                                               ? b
                                               : acc);

    Assert.That(aggregate.BlobName,
                Is.EqualTo("100"));
  }

  [Test]
  public async Task ConcatTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;

    // Pre-fetch blobs 1-3 to use as source2
    var blob1To3 = await Client!.BlobService.AsQueryable()
                                .Where(b => b.SessionId == sessionId)
                                .OrderBy(b => b.BlobName)
                                .Take(3)
                                .AsAsyncEnumerable()
                                .ToArrayAsync()
                                .ConfigureAwait(false);

    // Concat blobs 4-6 (server) with blobs 1-3 (source2): 6 blobs in server-first order
    var concat = await Client!.BlobService.AsQueryable()
                              .Where(b => b.SessionId == sessionId)
                              .OrderBy(b => b.BlobName)
                              .Skip(3)
                              .Take(3)
                              .Concat(blob1To3)
                              .AsAsyncEnumerable()
                              .Select(b => b.BlobName.TrimStart())
                              .ToArrayAsync()
                              .ConfigureAwait(false);

    // Concat blobs 1-3 with themselves: 6 blobs, duplicates preserved (no deduplication)
    var concatSelf = await Client!.BlobService.AsQueryable()
                                  .Where(b => b.SessionId == sessionId)
                                  .OrderBy(b => b.BlobName)
                                  .Take(3)
                                  .Concat(blob1To3)
                                  .AsAsyncEnumerable()
                                  .Select(b => b.BlobName.TrimStart())
                                  .ToArrayAsync()
                                  .ConfigureAwait(false);

    string[] expectedConcat     = ["4", "5", "6", "1", "2", "3"];
    string[] expectedConcatSelf = ["1", "2", "3", "1", "2", "3"];
    Assert.Multiple(() =>
                    {
                      Assert.That(concat,
                                  Is.EquivalentTo(expectedConcat));
                      Assert.That(concatSelf,
                                  Is.EquivalentTo(expectedConcatSelf));
                    });
  }

  [Test]
  public async Task IntersectTest()
  {
    var sessionId    = SessionHandle!.SessionInfo.SessionId;
    var blobComparer = new BlobStateComparer();

    // Pre-fetch blobs 4-8 to use as source2
    var blob4To8 = await Client!.BlobService.AsQueryable()
                                .Where(b => b.SessionId == sessionId)
                                .OrderBy(b => b.BlobName)
                                .Skip(3)
                                .Take(5)
                                .AsAsyncEnumerable()
                                .ToArrayAsync()
                                .ConfigureAwait(false);

    // Intersect blobs 1-5 with blobs 4-8: only blobs 4 and 5 are in both
    var intersect = await Client!.BlobService.AsQueryable()
                                 .Where(b => b.SessionId == sessionId)
                                 .OrderBy(b => b.BlobName)
                                 .Take(5)
                                 .Intersect(blob4To8)
                                 .AsAsyncEnumerable()
                                 .Select(b => b.BlobName.TrimStart())
                                 .ToArrayAsync()
                                 .ConfigureAwait(false);

    // Intersect with name comparer: same result
    var intersectWithComparer = await Client!.BlobService.AsQueryable()
                                             .Where(b => b.SessionId == sessionId)
                                             .OrderBy(b => b.BlobName)
                                             .Take(5)
                                             .Intersect(blob4To8,
                                                        blobComparer)
                                             .AsAsyncEnumerable()
                                             .Select(b => b.BlobName.TrimStart())
                                             .ToArrayAsync()
                                             .ConfigureAwait(false);

    string[] expected = ["4", "5"];
    Assert.Multiple(() =>
                    {
                      Assert.That(intersect,
                                  Is.EquivalentTo(expected));
                      Assert.That(intersectWithComparer,
                                  Is.EquivalentTo(expected));
                    });
  }

  [Test]
  public async Task ExceptTest()
  {
    var sessionId    = SessionHandle!.SessionInfo.SessionId;
    var blobComparer = new BlobStateComparer();

    // Pre-fetch blobs 4-8 to use as source2
    var blob4To8 = await Client!.BlobService.AsQueryable()
                                .Where(b => b.SessionId == sessionId)
                                .OrderBy(b => b.BlobName)
                                .Skip(3)
                                .Take(5)
                                .AsAsyncEnumerable()
                                .ToArrayAsync()
                                .ConfigureAwait(false);

    // Blobs 1-5 except blobs 4-8: only blobs 1, 2, 3 remain
    var except = await Client!.BlobService.AsQueryable()
                              .Where(b => b.SessionId == sessionId)
                              .OrderBy(b => b.BlobName)
                              .Take(5)
                              .Except(blob4To8)
                              .AsAsyncEnumerable()
                              .Select(b => b.BlobName.TrimStart())
                              .ToArrayAsync()
                              .ConfigureAwait(false);

    // Except with name comparer: same result
    var exceptWithComparer = await Client!.BlobService.AsQueryable()
                                          .Where(b => b.SessionId == sessionId)
                                          .OrderBy(b => b.BlobName)
                                          .Take(5)
                                          .Except(blob4To8,
                                                  blobComparer)
                                          .AsAsyncEnumerable()
                                          .Select(b => b.BlobName.TrimStart())
                                          .ToArrayAsync()
                                          .ConfigureAwait(false);

    string[] expected = ["1", "2", "3"];
    Assert.Multiple(() =>
                    {
                      Assert.That(except,
                                  Is.EquivalentTo(expected));
                      Assert.That(exceptWithComparer,
                                  Is.EquivalentTo(expected));
                    });
  }

  [Test]
  public async Task DefaultIfEmptyTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;
    var defaultBlob = new BlobState
                      {
                        BlobName = "default",
                      };

    // DefaultIfEmpty on a non-empty query: returns the 3 blobs unchanged
    var nonEmpty = await Client!.BlobService.AsQueryable()
                                .Where(b => b.SessionId == sessionId)
                                .OrderBy(b => b.BlobName)
                                .Take(3)
                                .DefaultIfEmpty()
                                .AsAsyncEnumerable()
                                .Select(b => b?.BlobName.TrimStart() ?? "null")
                                .ToArrayAsync()
                                .ConfigureAwait(false);

    // DefaultIfEmpty on an empty query: emits a single null element
    var empty = await Client!.BlobService.AsQueryable()
                             .Where(b => b.SessionId == sessionId && b.BlobName == "xyz")
                             .DefaultIfEmpty()
                             .AsAsyncEnumerable()
                             .ToArrayAsync()
                             .ConfigureAwait(false);

    // DefaultIfEmpty with a default value on an empty query: emits the default blob
    var emptyWithDefault = await Client!.BlobService.AsQueryable()
                                        .Where(b => b.SessionId == sessionId && b.BlobName == "xyz")
                                        .DefaultIfEmpty(defaultBlob)
                                        .AsAsyncEnumerable()
                                        .Select(b => b!.BlobName)
                                        .ToArrayAsync()
                                        .ConfigureAwait(false);

    string[]     resultNonEmpty           = ["1", "2", "3"];
    BlobState?[] resultEmpty              = [null];
    string[]     expectedEmptyWithDefault = ["default"];
    Assert.Multiple(() =>
                    {
                      Assert.That(nonEmpty,
                                  Is.EquivalentTo(resultNonEmpty));
                      Assert.That(empty,
                                  Is.EqualTo(resultEmpty));
                      Assert.That(emptyWithDefault,
                                  Is.EqualTo(expectedEmptyWithDefault));
                    });
  }

  [Test]
  public async Task ReverseTest()
  {
    var sessionId = SessionHandle!.SessionInfo.SessionId;

    // Reverse the first 5 blobs (ascending): expect them in descending order
    var reversed = await Client!.BlobService.AsQueryable()
                                .Where(b => b.SessionId == sessionId)
                                .OrderBy(b => b.BlobName)
                                .Take(5)
                                .Reverse()
                                .AsAsyncEnumerable()
                                .Select(b => b.BlobName.TrimStart())
                                .ToArrayAsync()
                                .ConfigureAwait(false);

    string[] expectedReversed = ["5", "4", "3", "2", "1"];
    Assert.That(reversed,
                Is.EqualTo(expectedReversed));
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
