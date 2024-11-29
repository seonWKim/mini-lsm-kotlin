package org.github.seonwkim.lsm.storage

import org.github.seonwkim.lsm.sstable.Sstable
import java.util.concurrent.ConcurrentHashMap

/**
 * Snapshot of the LSM storage state.
 *
 * This class represents a snapshot of the [LsmStorageState], capturing the state of the LSM storage system
 * at a specific point in time. It includes information about the SSTables at level 0, the SSTables at other levels,
 * and a concurrent map of all SSTables.
 *
 * @property l0Sstables The list of SSTable IDs at level 0.
 * @property levels The list of SSTable levels, each represented by an `SstLevel` object.
 * @property sstables A concurrent map of SSTable IDs to `Sstable` objects. Changes to this map may be applied by other threads if concurrently modified.
 */
data class LsmStorageStateDiskSnapshot(
    val l0Sstables: List<Int>,
    val levels: List<SstLevel>,
    val sstables: ConcurrentHashMap<Int, Sstable>
)
