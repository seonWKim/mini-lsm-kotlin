package org.seonwkim.lsm.sstable

import com.github.benmanes.caffeine.cache.Caffeine
import org.seonwkim.lsm.block.Block
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

class BlockCache(
    val size: Long = 1000,
    val expireAfterWrite: Duration = Duration.ofMinutes(10)
) {
    /**
     * We can calculate the size of the cache by using [BlockBuilder#blockSize] and the number of elements inside cache
     */
    private val cache = Caffeine.newBuilder()
        .maximumSize(size)
        .expireAfterWrite(expireAfterWrite)
        .build<BlockCacheKey, Block>()

    fun getOrDefault(key: BlockCacheKey, defaultValue: () -> Block): Block {
        return cache.get(key) {
            val block = defaultValue()
            block
        }
    }

    fun copy(): BlockCache {
        val newCache = BlockCache()
        newCache.cache.putAll(cache.asMap())
        return newCache
    }
}

data class BlockCacheKey(val id: Int, val blockIdx: Int)
