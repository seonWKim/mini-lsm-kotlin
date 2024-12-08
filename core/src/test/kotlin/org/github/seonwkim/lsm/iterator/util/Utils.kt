package org.github.seonwkim.lsm.iterator.util

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.lsm.sstable.BlockCache
import org.github.seonwkim.lsm.sstable.SsTableBuilder
import org.github.seonwkim.lsm.sstable.Sstable
import java.nio.file.Path


fun generateSst(
    id: Int,
    path: Path,
    data: List<Pair<ComparableByteArray, ComparableByteArray>>,
    blockCache: BlockCache?
): Sstable {
    val builder = SsTableBuilder(128)
    data.forEach { (key, value) ->
        builder.add(key, value)
    }
    return builder.build(
        id = id,
        blockCache = blockCache,
        path = path
    )
}
