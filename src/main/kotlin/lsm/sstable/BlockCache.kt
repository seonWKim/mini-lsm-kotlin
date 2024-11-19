package org.seonWKim.lsm.sstable

import org.seonWKim.lsm.block.Block
import java.util.concurrent.ConcurrentHashMap

class BlockCache {
    private val cache: ConcurrentHashMap<Pair<Int, Int>, Block> = ConcurrentHashMap()
}
