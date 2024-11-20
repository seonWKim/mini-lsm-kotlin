package org.seonwkim.lsm.sstable

import org.seonwkim.lsm.block.Block
import java.util.concurrent.ConcurrentHashMap

// TODO: should we limit the max cache limit in bytes?
class BlockCache(
    size: Int = 1000
) {
    private val cache: ConcurrentHashMap<BlockCacheKey, Block> = ConcurrentHashMap(size)

    fun getOrDefault(key: BlockCacheKey, defaultValue: () -> Block): Block {
        return cache[key].let {
            val block = defaultValue()
            cache[key] = block
            block
        }
    }

    fun copy(): BlockCache {
        val newCache = BlockCache()
        newCache.cache.putAll(this.cache)
        return newCache
    }
}

data class BlockCacheKey(val id: Int, val blockIdx: Int)
