using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BPlusTree
{
    /// <summary>
    /// Basic algorithms used to implement <see cref="BTree{TKey, TValue}"/>.
    /// </summary>
    /// <remarks>
    /// This functions are not present as private methods of <see cref="BTree{TKey, TValue}"/> 
    /// because they need to be instantiated, for interior nodes and leaf nodes.
    /// Furthermore, the functions do not touch any instance members 
    /// of <see cref="BTree{TKey, TValue}"/> directly, to aid in understanding the code.
    /// </remarks>
    internal static partial class BTreeCore
    {
        /// <summary>
        /// Find the index within a node where an entry can be inserted for the given key.
        /// </summary>
        /// <typeparam name="TValue">The type of data value to insert along with the key. 
        /// Must be <see cref="NodeLink"/> for internal nodes.
        /// </typeparam>
        /// <param name="key">The key to search for. </param>
        /// <param name="forUpperBound">If false, this method returns the "lower bound" index.
        /// If true, this method returns the "upper bound" index.
        /// </param>
        /// <param name="entries">The entries in the node. </param>
        /// <param name="numEntries">The number of active entries in the node. </param>
        /// <param name="found">Set to true if <paramref name="key"/> is exactly 
        /// found in the node, i.e. <paramref name="keyComparer"/> returns zero
        /// when comparing keys.
        /// </param>
        /// <returns>
        /// For "lower bound": the first index where an entry with the given key, 
        /// or a preceding key, can be inserted without violating ordering.  For 
        /// "upper bound": the first index where an entry with a following (greater) key 
        /// can be inserted without violating ordering. For internal nodes, the index is 
        /// shifted down by one, so it starts from 0 while the keys are stored starting 
        /// at index 1: thus a "lower bound" search on internal nodes yields the index to 
        /// follow down the B+Tree.
        /// </returns>
        internal static int SearchKeyWithinNode<TKey, TValue>(IComparer<TKey> keyComparer,
                                                              TKey key,
                                                              bool forUpperBound,
                                                              Entry<TKey, TValue>[] entries,
                                                              int numEntries,
                                                              out bool found)
        {
            // Keys in internal nodes are stored starting from index 1,
            // but we still return 0-based index
            int shift = (typeof(TValue) == typeof(NodeLink)) ? 1 : 0;

            // The closed interval [left,right] brackets the returned index
            int left = shift;
            int right = numEntries;

            bool hasEqualKey = false;

            // Bisect until the interval brackets only one choice of index
            while (left != right)
            {
                // B+Tree order is capped so this index calculation cannot overflow.
                // If right == left + 1, mid is always biased down to be left,
                // thus it will be a valid index in reading the array below.
                int mid = (left + right) >> 1;

                int comparison = keyComparer.Compare(entries[mid].Key, key);
  
                // This binary search will be guaranteed to compare
                // against the search key if it exists in the node.
                hasEqualKey |= (comparison == 0);
                
                if (comparison < 0 || (forUpperBound && (comparison == 0)))
                    left = mid + 1;
                else
                    right = mid;
            }

            found = hasEqualKey;
            return left - shift;
        }

        /// <summary>
        /// Cast an object reference as an interior (non-leaf) node of the B+Tree.
        /// </summary>
        public static Entry<TKey, NodeLink>[] AsInteriorNode<TKey>(object node)
            => (Entry<TKey, NodeLink>[])node;

        /// <summary>
        /// Throw the exception about accessing an invalid entry of a B+Tree from an enumerator.
        /// </summary>
        public static void ThrowInvalidValueInEnumerator()
        {
            throw new InvalidOperationException(
                "Cannot retrieve the value from this enumerator because it is not pointing to a valid entry in the B+Tree. ");
        }

        /// <summary>
        /// Throw an exception if <see cref="BTree{TKey, TValue}.Enumerator" /> has
        /// been invalidated.
        /// </summary>
        public static void CheckEnumeratorVersion(ref BTreePath path, int _version)
        {
            if (path.Version != _version)
            {
                throw new InvalidOperationException(
                    "Cannot use this enumerator any longer because the B+Tree it is referring to has been modified. ");
            }
        }
    }
}
