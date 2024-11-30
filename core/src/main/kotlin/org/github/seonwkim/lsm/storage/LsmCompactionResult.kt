package org.github.seonwkim.lsm.storage

/**
 * Represents the result of a compaction operation in the LSM storage system.
 *
 * @property snapshot The state of the LSM storage system after the compaction.
 * @property sstIdsToRemove The set of SSTable IDs that were removed during the compaction.
 * @property l0Compacted Indicates whether the L0 level was compacted.
 */
data class LsmCompactionResult(
    val snapshot: LsmStorageSstableSnapshot,
    val sstIdsToRemove: HashSet<Int>,
    val l0Compacted: Boolean = false
)
