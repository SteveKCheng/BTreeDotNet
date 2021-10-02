using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace BPlusTree
{
    public class BTreeTests
    {
        [Fact]
        public void Test1()
        {
            var btree = new BTree<int, int>(8, Comparer<int>.Default);

            var random = new Random(37);

            var seen = new HashSet<int>();
            var items = new int[300];
            for (int i = 0; i < items.Length; ++i)
            {
                int number;
                do
                {
                    number = random.Next(0, 10000);
                } while (!seen.Add(number));

                items[i] = number;
            }

            foreach (var number in items)
                btree.Add(number, number);

            var sortedItems = items.OrderBy(x => x).ToArray();

            var outputs = btree.ToArray();
            Assert.All(outputs, item => Assert.Equal(item.Key, item.Value));
            Assert.Equal(sortedItems, outputs.Select(item => item.Key));
        }
    }
}
