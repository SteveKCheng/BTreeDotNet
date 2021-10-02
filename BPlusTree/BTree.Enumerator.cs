using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BPlusTree
{
    public partial class BTree<TKey, TValue>
    {
        /// <summary>
        /// Iterates through the key/value pairs inside <see cref="BTree{TKey, TValue}"/>,
        /// in the order defined by the key comparer.
        /// </summary>
        public struct Enumerator : IEnumerator<KeyValuePair<TKey, TValue>>
        {
            /// <inheritdoc cref="IEnumerator{T}.Current" />
            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    if (!_valid)
                        throw new InvalidOperationException("Cannot retrieve the current value of the enumerator when it has ended. ");
                    return _current;
                }
            }

            /// <inheritdoc cref="IEnumerator.Current" />
            object IEnumerator.Current => Current;

            public void Dispose()
            {
                _valid = false;
                _path.Dispose();
            }

            /// <inheritdoc cref="IEnumerator.MoveNext" />
            public bool MoveNext()
            {
                if (!_valid)
                    return false;

                int depth = _path.Depth;
                ref var step = ref _path.Steps[depth];

                // If the index went past all the active slots in the leaf node, 
                // then we need to trace the path back up the B+Tree to find
                // find the next neighboring leaf node.
                if (step.Index >= _entriesCount)
                {
                    for (int level = depth; level > 0; --level)
                    {
                        ref var parentStep = ref _path.Steps[level - 1];
                        var parentNode = BTreeCore.AsInteriorNode<TKey>(parentStep.Node!);
                        int parentIndex = parentStep.Index;
                        if (parentIndex + 1 < parentNode.Length)
                        {
                            ref var currentLink = ref parentNode[parentIndex + 1].Value;

                            // Found the pivot for the right neighbor to the current leaf node
                            if (currentLink.Child != null)
                            {
                                ++parentStep.Index;
                                ResetPathPartially(currentLink, level);
                                goto hasItem;
                            }
                        }
                    }

                    // The current leaf node is the last one and has no right neighbor.
                    _valid = false;
                    return false;
                }

            hasItem:
                var leafNode = AsLeafNode(step.Node!);
                ref var entry = ref leafNode[step.Index];
                _current = new KeyValuePair<TKey, TValue>(entry.Key, entry.Value);
                ++step.Index;
                return true;
            }

            /// <summary>
            /// Starting from a given level of the B+Tree, take the left-most path
            /// down to the first entry in a leaf node.
            /// </summary>
            private void ResetPathPartially(NodeLink currentLink, int level)
            {
                int depth = _path.Depth;
                while (level < depth)
                {
                    _path.Steps[level] = new BTreeStep(currentLink.Child!, 0);
                    currentLink = BTreeCore.AsInteriorNode<TKey>(currentLink.Child!)[0].Value;
                    ++level;
                }

                _path.Steps[depth] = new BTreeStep(currentLink.Child!, 0);
                _entriesCount = currentLink.EntriesCount;
            }

            /// <inheritdoc cref="IEnumerator.Reset" />
            public void Reset()
            {
                int depth = _path.Depth;

                if (depth != _owner.Depth)
                {
                    _path.Dispose();
                    _path = _owner.NewPath();
                }

                ResetPathPartially(_owner._root, 0);
                _valid = true;
            }

            /// <summary>
            /// Set to false when <see cref="MoveNext" /> moves past the last leaf node.
            /// </summary>
            private bool _valid;

            /// <summary>
            /// Remembers the path from the root of the B+Tree down to the leaf node
            /// so that the enumerator can move to the neighboring leaf node.
            /// </summary>
            private BTreePath _path;

            /// <summary>
            /// Copy of the item in the B+Tree updated by <see cref="MoveNext" />.
            /// </summary>
            private KeyValuePair<TKey, TValue> _current;

            /// <summary>
            /// Cached count of the entries in the current leaf node.
            /// </summary>
            private int _entriesCount;

            /// <summary>
            /// The B+Tree that this enumerator comes from.
            /// </summary>
            private readonly BTree<TKey, TValue> _owner;

            /// <summary>
            /// Prepare to enumerate items in the B+Tree starting from the first,
            /// when ordered by item key.
            /// </summary>
            /// <param name="owner">The B+Tree to enumerate items from. </param>
            public Enumerator(BTree<TKey, TValue> owner)
            {
                _owner = owner;
                _path = default;
                _entriesCount = 0;
                _valid = false;
                _current = default;

                Reset();
            }
        }

        public Enumerator GetEnumerator() => new Enumerator(this);

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
            => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <inheritdoc cref="ICollection{T}.CopyTo(T[], int)"/>
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            if (array.Length - arrayIndex < Count)
                throw new ArgumentOutOfRangeException(nameof(array), "Array is not large enough to hold all the items in this B+Tree. ");

            foreach (var item in this)
                array[arrayIndex++] = item;
        }
    }
}
