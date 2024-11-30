package org.github.seonwkim.lsm.storage

/**
 * Snapshot of the LSM SST.
 *
 * This class represents a snapshot of the [LsmStorageState], capturing the state of the LSM storage system
 * at a specific point in time. It includes information about the SSTables at level 0, the SSTables at other levels,
 * and a concurrent map of all SSTables.
 *
 * @property l0Sstables The list of SSTable IDs at level 0.
 * @property levels The list of SSTable levels, each represented by an `SstLevel` object. Represented as tier when tiered compaction is used.
 */
data class LsmStorageSstableSnapshot(
    val l0Sstables: List<Int>,
    val levels: List<SstLevel>,
)
