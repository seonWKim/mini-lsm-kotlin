package org.seonwkim.lsm.sstable

import org.seonwkim.lsm.block.Block
import java.util.concurrent.ConcurrentHashMap

class BlockCache {
    private val cache: ConcurrentHashMap<Pair<Int, Int>, Block> = ConcurrentHashMap()
}
