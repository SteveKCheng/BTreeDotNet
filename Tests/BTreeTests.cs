// © 2001 Steve Cheng.
// See LICENSE.txt for copyright licensing of this work.

using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace GoldSaucer.BTree
{
    public class BTreeTests
    {
        private static int[] GenerateRandomSequence(Random random, int count, bool unique)
        {
            var items = new int[count];

            var seen = unique ? new HashSet<int>() : null;

            for (int i = 0; i < count; ++i)
            {
                int number;
                do
                {
                    number = random.Next(0, 10000);
                } while (seen != null ? !seen.Add(number) : false);

                items[i] = number;
            }

            return items;
        }

        [Theory]
        [InlineData(37)]
        [InlineData(55)]
        [InlineData(99)]
        public void InsertItems(int seed)
        {
            var btree = new BTreeMap<int, int>(8, Comparer<int>.Default);
            var random = new Random(seed);

            var items = GenerateRandomSequence(random, 300, true);

            foreach (var number in items)
                btree.Add(number, number);

            Assert.Equal(items.Length, btree.Count);

            var sortedItems = items.OrderBy(x => x).ToArray();

            var outputs = btree.ToArray();
            Assert.All(outputs, item => Assert.Equal(item.Key, item.Value));
            Assert.Equal(sortedItems, outputs.Select(item => item.Key));
        }

        [Theory]
        [InlineData(37)]
        [InlineData(55)]
        [InlineData(99)]
        public void InsertAndDeleteItems(int seed)
        {
            var btree = new BTreeMap<int, int>(8, Comparer<int>.Default);
            var random = new Random(seed);
            var items = GenerateRandomSequence(random, 300, true);

            var removedItems = new List<int>();

            for (int i = 0; i < items.Length; ++i)
            {
                var number = items[i];
                btree.Add(number, number);

                if ((i % 5) == 4)
                {
                    var numberToRemove = items[i - 4];
                    btree.Remove(numberToRemove);
                    removedItems.Add(numberToRemove);
                }
            }

            var sortedItems = items.Except(removedItems).OrderBy(x => x).ToArray();

            Assert.Equal(sortedItems.Length, btree.Count);

            var outputs = btree.ToArray();

            Assert.All(outputs, item => Assert.Equal(item.Key, item.Value));
            Assert.Equal(sortedItems, outputs.Select(item => item.Key));

            Assert.All(sortedItems, number => Assert.True(btree.TryGetValue(number, out int v) && v == number));
            Assert.All(removedItems, number => Assert.False(btree.ContainsKey(number)));
        }

        [Fact]
        public void EnumerateAndDelete()
        {
            var btree = new BTreeMap<int, int>(8, Comparer<int>.Default);
            var random = new Random(37);
            var items = GenerateRandomSequence(random, 300, true);

            var removedItems = new List<int>();

            for (int i = 0; i < items.Length; ++i)
            {
                var number = items[i];
                btree.Add(number, number);
            }

            var enumerator = btree.GetEnumerator();
            try
            {
                int i = 0;
                int previousItem = -1;

                Assert.False(enumerator.MovePrevious());
                Assert.False(enumerator.MovePrevious());

                while (enumerator.MoveNext())
                {
                    if (removedItems.Count > 0)
                        Assert.NotEqual(removedItems[^1], enumerator.Current.Key);

                    Assert.True(previousItem < enumerator.Current.Key);

                    if ((i % 7) == 4)
                    {
                        removedItems.Add(enumerator.Current.Key);
                        enumerator.RemoveCurrent();
                        enumerator.MovePrevious();
                        Assert.Equal(previousItem, enumerator.Current.Key);
                    }

                    previousItem = enumerator.Current.Key;
                    ++i;
                }

                Assert.True(i > 0);
                Assert.False(enumerator.MoveNext());
                Assert.True(enumerator.MovePrevious());
                Assert.Equal(previousItem, enumerator.Current.Key);
                Assert.False(enumerator.MoveNext());
            }
            finally
            {
                enumerator.Dispose();
            }

            var sortedItems = items.Except(removedItems).OrderBy(x => x).ToArray();

            Assert.Equal(sortedItems.Length, btree.Count);

            var outputs = btree.ToArray();

            Assert.All(outputs, item => Assert.Equal(item.Key, item.Value));
            Assert.Equal(sortedItems, outputs.Select(item => item.Key));

            Assert.All(sortedItems, number => Assert.True(btree.TryGetValue(number, out int v) && v == number));
            Assert.All(removedItems, number => Assert.False(btree.ContainsKey(number)));
        }
    }
}
