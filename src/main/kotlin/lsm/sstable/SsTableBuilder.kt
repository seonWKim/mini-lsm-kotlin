package org.seonWKim.lsm.sstable

import com.google.common.hash.Hashing
import org.seonWKim.common.ComparableByteArray
import org.seonWKim.lsm.block.BlockBuilder
import org.seonWKim.lsm.block.BlockKey
import java.nio.file.Path

class SsTableBuilder(
    private val blockSize: Int,
) {
    private val builder: BlockBuilder
    private val firstKey: BlockKey
    private val lastKey: BlockKey
    private val data: MutableList<Byte>

    // private val meta: List<BlockMeta>,
    private val keyHashes: MutableList<Int>

    init {
        this.builder = BlockBuilder(blockSize)
        this.firstKey = BlockKey.empty()
        this.lastKey = BlockKey.empty()
        this.data = mutableListOf()
        this.keyHashes = mutableListOf()
    }

    companion object {
        private val hasher = Hashing.murmur3_32_fixed().newHasher()
    }


    fun add(key: BlockKey, value: ComparableByteArray) {
        if (this.firstKey.isEmpty()) {
            firstKey.setFromBlockKey(key)
        }

        val hasher = Hashing.murmur3_32_fixed().newHasher().let { it.putBytes(value.getByteArray()) }
        keyHashes.add(hasher.hash().asInt())

        if (builder.add(key, value)) {
            lastKey.setFromBlockKey(key)
            return
        }

        // create a new block builder and append block data
        finishBlock()

        // add the key-value pair to the next block
        if (builder.add(key, value)) {
            throw IllegalStateException("Unable to add key($key) and value($value) to builder")
        }
        firstKey.setFromBlockKey(key)
        lastKey.setFromBlockKey(key)
    }

    fun estimatedSize(): Int {
        return data.size
    }

    fun finishBlock() {
        // TODO
    }

    // build SSTable and write it to given path
    fun build(): SsTable {
        // TODO
        return SsTable()
    }

    fun buildForTest(path: Path): SsTable {
        // TODO
        return build()
    }
}

class SsTable {

}
