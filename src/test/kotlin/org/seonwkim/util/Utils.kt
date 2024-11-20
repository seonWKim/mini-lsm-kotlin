package org.seonwkim.util

import org.seonwkim.common.ComparableByteArray
import org.seonwkim.common.TimestampedKey
import org.seonwkim.lsm.LsmStorageOptions
import org.seonwkim.lsm.sstable.BlockCache
import org.seonwkim.lsm.sstable.SsTable
import org.seonwkim.lsm.sstable.SsTableBuilder
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

fun lsmStorageOptionForTest(): LsmStorageOptions = LsmStorageOptions(
    blockSize = 4096,
    targetSstSize = 2 shl 20,
    numMemTableLimit = 10
)