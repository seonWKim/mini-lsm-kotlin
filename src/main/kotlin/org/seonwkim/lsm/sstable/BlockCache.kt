package org.seonwkim.lsm.sstable

import org.seonwkim.lsm.block.Block
import java.util.concurrent.ConcurrentHashMap

class BlockCache {
    private val cache: ConcurrentHashMap<BlockCacheKey, Block> = ConcurrentHashMap()

    fun getOrDefault(key: BlockCacheKey, defaultValue: () -> Block): Block {
        return cache[key].let {
            val block = defaultValue()
            cache[key] = block
            block
        }
    }
}

data class BlockCacheKey(val id: Int, val blockIdx: Int)
