package org.github.seonwkim.lsm.iterator.util

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.TimestampedKey
import org.github.seonwkim.lsm.storage.LsmStorageOptions
import org.github.seonwkim.lsm.sstable.BlockCache
import org.github.seonwkim.lsm.sstable.SsTable
import org.github.seonwkim.lsm.sstable.SsTableBuilder
import java.nio.file.Path


fun generateSst(
    id: Int,
    path: Path,
    data: List<Pair<ComparableByteArray, ComparableByteArray>>,
    blockCache: BlockCache?
): SsTable {
    val builder = SsTableBuilder(128)
    data.forEach { (key, value) ->
        builder.add(TimestampedKey(key), value)
    }
    return builder.build(
        id = id,
        blockCache = blockCache,
        path = path
    )
}