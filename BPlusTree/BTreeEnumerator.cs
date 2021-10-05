using System;
using System.Collections;
using System.Collections.Generic;

namespace BPlusTree
{
    internal static partial class BTreeCore
    {
        public static void CopyFromEnumeratorToArray<TItem, TEnumerator>
            (TEnumerator enumerator, int count, TItem[] array, int arrayIndex)
            where TEnumerator : IEnumerator<TItem>
        {
            try
            {
                if (array.Length - arrayIndex < count)
                    throw new ArgumentOutOfRangeException(nameof(array), "Array is not large enough to hold all the items from this B+Tree. ");

                while (enumerator.MoveNext())
                    array[arrayIndex++] = enumerator.Current;
            }
            finally
            {
                enumerator.Dispose();
            }
        }
    }

    /// <summary>
    /// Iterates through the key/value pairs inside <see cref="BTree{TKey, TValue}"/>,
    /// in the order defined by the key comparer.
    /// </summary>
    public struct BTreeEnumerator<TKey, TValue> : IEnumerator<KeyValuePair<TKey, TValue>>
    {
        /// <inheritdoc cref="IEnumerator{T}.Current" />
        public KeyValuePair<TKey, TValue> Current
        {
            get
            {
                if (!_valid)
                    BTreeCore.ThrowInvalidValueInEnumerator();
                return _current;
            }
        }

        /// <inheritdoc cref="IEnumerator.Current" />
        object IEnumerator.Current => Current;

        /// <summary>
        /// Frees up temporary scratchpad memory used for iterating through
        /// the B+Tree.
        /// </summary>
        public void Dispose()
        {
            _current = default;
            _valid = false;
            _ended = false;
            _path.Dispose();
        }

        /// <summary>
        /// Move <see cref="_path" /> to be positioned at the first entry
        /// of the next leaf node (right neighbor), if it exists.
        /// </summary>
        /// <returns>
        /// True if moving to the next leaf node is successful.
        /// False if there is no next leaf node; in this case 
        /// <see cref="_path"/> remains unchanged.
        /// </returns>
        private bool MoveToNextLeafNode()
        {
            for (int level = _path.Depth; level > 0; --level)
            {
                ref var parentStep = ref _path[level - 1];
                var parentNode = BTreeCore.AsInteriorNode<TKey>(parentStep.Node!);
                int parentIndex = parentStep.Index;
                if (parentIndex + 1 < parentNode.Length)
                {
                    ref var currentLink = ref parentNode[parentIndex + 1].Value;

                    // Found the pivot for the right neighbor to the current leaf node
                    if (currentLink.Child != null)
                    {
                        ++parentStep.Index;
                        ResetPathPartially(node: currentLink, level: level, left: true);
                        return true;
                    }
                }
            }

            // The current leaf node is the last one and has no right neighbor.
            return false;
        }

        /// <summary>
        /// Move <see cref="_path" /> to be positioned at the last entry
        /// of the previous leaf node (left neighbor), if it exists.
        /// </summary>
        /// <returns>
        /// True if moving to the previous leaf node is successful.
        /// False if there is no previous leaf node; in this case 
        /// <see cref="_path"/> remains unchanged.
        /// </returns>
        private bool MoveToPreviousLeafNode()
        {
            for (int level = _path.Depth; level > 0; --level)
            {
                ref var parentStep = ref _path[level - 1];
                var parentNode = BTreeCore.AsInteriorNode<TKey>(parentStep.Node!);
                int parentIndex = parentStep.Index;
                if (parentIndex > 0)
                {
                    ref var currentLink = ref parentNode[parentIndex - 1].Value;

                    --parentStep.Index;
                    ResetPathPartially(node: currentLink, level: level, left: false);
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Move forwards to the following entry in the B+Tree.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The first call to this method,
        /// after this enumerator has been initialized to be
        /// "at the beginning", via <see cref="Reset"/> or the 
        /// <see cref="BTreeEnumerator"/> constructor, will cause
        /// <see cref="Current" /> to output the first entry of the B+Tree.
        /// </para>
        /// <para>
        /// Calls to this method may be mixed with <see cref="MovePrevious" />.
        /// </para>
        /// </remarks>
        /// <returns>
        /// True if this enumerator now points to the following entry;
        /// false if it has reached the end.  
        /// </returns>
        public bool MoveNext()
        {
            if (_ended)
                return false;

            BTreeCore.CheckEnumeratorVersion(ref _path, Owner._version);

            ref var step = ref _path.Leaf;

            // Do not increment step.Index on the very first call (after Reset)
            if (_valid)
                ++step.Index;

            // If the index went past all the active slots in the leaf node, 
            // then we need to trace the path back up the B+Tree to find
            // find the next neighboring leaf node.
            if (step.Index >= _entriesCount && !MoveToNextLeafNode())
            {
                _current = default;
                _valid = false;
                _ended = true;
                return false;
            }

            var leafNode = BTree<TKey, TValue>.AsLeafNode(step.Node!);
            ref var entry = ref leafNode[step.Index];
            _current = new KeyValuePair<TKey, TValue>(entry.Key, entry.Value);
            _valid = true;
            return true;
        }

        /// <summary>
        /// Move backwards to preceding entry in the B+Tree.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The first call to this method,
        /// after this enumerator has been initialized to be
        /// "at the end", via <see cref="Reset"/> or the 
        /// <see cref="BTreeEnumerator"/> constructor, will cause
        /// <see cref="Current" /> to output the last entry of the B+Tree.
        /// </para>
        /// <para>
        /// Calls to this method may be mixed with <see cref="MoveNext" />.
        /// </para>
        /// </remarks>
        /// <returns>
        /// True if this enumerator now points to the preceding entry;
        /// false if there is none.  
        /// </returns>
        public bool MovePrevious()
        {
            BTreeCore.CheckEnumeratorVersion(ref _path, Owner._version);

            ref var step = ref _path.Leaf;
            if (step.Index == 0 && !MoveToPreviousLeafNode())
            {
                _current = default;
                _valid = false;
                _ended = false;
                return false;
            }

            --step.Index;
            var leafNode = BTree<TKey, TValue>.AsLeafNode(step.Node!);
            ref var entry = ref leafNode[step.Index];
            _current = new KeyValuePair<TKey, TValue>(entry.Key, entry.Value);
            _valid = true;
            _ended = false;
            return true;
        }

        /// <summary>
        /// Remove the entry that this enumerator is currently pointing to.
        /// </summary>
        /// <remarks>
        /// After this method completes successfully, this enumerator will not be pointing
        /// to any valid entry.  Call <see cref="MoveNext" /> or <see cref="MovePrevious" />
        /// to get this enumerator to point to the entry following or preceduing 
        /// the one that has been removed, respectively.
        /// </remarks>
        public void RemoveCurrent()
        {
            if (!_valid)
                throw new InvalidOperationException("This enumerator is not pointing to any entry from the B+Tree that can be removed. ");

            BTreeCore.CheckEnumeratorVersion(ref _path, Owner._version);
            Owner.DeleteAtPath(ref _path);

            _current = default;
            _valid = false;
        }

        /// <summary>
        /// Insert an entry into the B+Tree using this
        /// enumerator as the hint to the location.
        /// </summary>
        /// <param name="key">The desired key to insert. </param>
        /// <param name="value">The value associated to the key to insert. </param>
        /// <returns>
        /// True if the entry may be inserted at the current location and has
        /// been inserted successfully.  False if the entry may not be inserted
        /// at the current location because it would disrupt the ordering 
        /// of keys in the B+Tree, i.e. the current location is not valid
        /// as a hint.
        /// </returns>
        public bool TryInsertBefore(TKey key, TValue value)
        {
            if (!_ended)
            {
                if (!_valid)
                    return false;

                if (Owner._keyComparer.Compare(key, _current.Key) > 0)
                    return false;
            }

            if (BTree<TKey, TValue>.TryGetPrecedingKey(ref _path, out var precedingKey) &&
                Owner._keyComparer.Compare(precedingKey, key) > 0)
                return false;

            Owner.InsertAtPath(key, value, ref _path);
            _current = new KeyValuePair<TKey, TValue>(key, value);

            return true;
        }

        /// <summary>
        /// Modifies <see cref="_path"/>
        /// to be the left-most path
        /// or right-most path starting from a given of the B+Tree.
        /// </summary>
        /// <param name="node">Points to the starting node existing 
        /// at the level of the B+tree given by <paramref name="level" />.
        /// </param>
        /// <param name="level">The level of the B+Tree to start moving downwards from.
        /// </param>
        /// <param name="left">True to take left-most path; false to take the right-most path.
        /// </param>
        private void ResetPathPartially(NodeLink node, int level, bool left)
        {
            int depth = _path.Depth;
            int index;
            while (level < depth)
            {
                index = left ? 0 : node.EntriesCount;
                _path[level] = new BTreeStep(node.Child!, index);
                node = BTreeCore.AsInteriorNode<TKey>(node.Child!)[index].Value;
                ++level;
            }

            index = left ? 0 : node.EntriesCount;
            _path.Leaf = new BTreeStep(node.Child!, index);
            _entriesCount = node.EntriesCount;
        }

        /// <inheritdoc cref="IEnumerator.Reset" />.
        public void Reset() => Reset(true);

        /// <summary>
        /// Reset this enumerator to either the beginning or end
        /// of the B+Tree.
        /// </summary>
        /// <param name="toBeginning">
        /// If true, resets to the beginning of the B+Tree, so that the next call
        /// to <see cref="MoveNext" /> retrieves the first entry
        /// of the B+Tree.  If false, resets to the end, so that the
        /// next call to <see cref="MovePrevious" /> retrieves
        /// the last entry of the B+Tree.
        /// </param>
        public void Reset(bool toBeginning)
        {
            var owner = Owner;
            int depth = _path.Depth;

            if (depth != owner.Depth)
            {
                _path.Dispose();
                _path = owner.NewPath();
            }
            else
            {
                _path.Version = owner._version;
            }

            ResetPathPartially(node: owner._root, level: 0, left: toBeginning);
            _current = default;
            _valid = false;
            _ended = !toBeginning;
        }

        /// <summary>
        /// True if this enumerator is pointing to a valid entry,
        /// so that the <see cref="Current" /> property can be queried.
        /// </summary>
        /// <remarks>
        /// This flag is the same as what has been returned in the last
        /// call to <see cref="MovePrevious"/> or <see cref="MoveNext"/>.
        /// If there has been no call to those methods after resetting
        /// or initializing this enumerator, this flag is false.
        /// </remarks>
        public bool IsValid => _valid;

        /// <summary>
        /// Backing field for <see cref="IsValid" />.
        /// </summary>
        /// <remarks>
        /// If this member is false while <see cref="_ended"/> is false, that means
        /// this instance has just been reset to the beginning of the B+Tree,
        /// and the first entry will be reported from the next call to
        /// <see cref="MoveNext" />.  If <see cref="_ended" /> is true
        /// then this member is necessarily false.
        /// </remarks>
        private bool _valid;

        /// <summary>
        /// Set to ended when <see cref="MoveNext" /> moves past the last leaf node.
        /// </summary>
        private bool _ended;

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
        public BTree<TKey, TValue> Owner { get; }

        /// <summary>
        /// Prepare to enumerate items in the B+Tree ordered by item key.
        /// </summary>
        /// <param name="owner">The B+Tree to enumerate items from. </param>
        /// <param name="toBeginning">
        /// If true, the enumerator is positioned to the beginning of the B+Tree, 
        /// so that the following call to <see cref="MoveNext" /> retrieves the first entry
        /// of the B+Tree.  If false, the enumerator is positioned to the end, so 
        /// that the next call to <see cref="MovePrevious" /> retrieves
        /// the last entry of the B+Tree.
        /// </param>
        internal BTreeEnumerator(BTree<TKey, TValue> owner, bool toBeginning)
        {
            Owner = owner;
            _path = default;
            _entriesCount = 0;
            _current = default;
            _valid = false;
            _ended = false;

            Reset(toBeginning);
        }
    }
}
