package org.seonwkim.lsm.sstable

import org.seonwkim.common.*
import org.seonwkim.lsm.block.*
import java.nio.file.Path

class SsTableBuilder(
    private val blockSize: Int,
) {
    private var builder: BlockBuilder
    private var firstKey: BlockKey
    private var lastKey: BlockKey
    private val data: ComparableByteArray

    val meta: MutableList<BlockMeta>
    private val keyHashes: MutableList<Int>
    private var maxTimestamp: Long

    init {
        this.builder = BlockBuilder(blockSize)
        this.firstKey = BlockKey.empty()
        this.lastKey = BlockKey.empty()
        this.data = ComparableByteArray.new()
        this.meta = mutableListOf()
        this.keyHashes = mutableListOf()
        this.maxTimestamp = 0L
    }

    fun add(key: BlockKey, value: ComparableByteArray) {
        if (this.firstKey.isEmpty()) {
            firstKey.setFromBlockKey(key)
        }

        if (key.timestamp() > maxTimestamp) {
            maxTimestamp = key.timestamp()
        }
        keyHashes.add(murmurHash(value))

        if (builder.add(key, value)) {
            lastKey.setFromBlockKey(key)
            return
        }

        // create a new block builder and append block data
        finishBlock()

        // add the key-value pair to the next block
        if (!builder.add(key, value)) {
            throw IllegalStateException("Unable to add key($key) and value($value) to builder")
        }
        firstKey.setFromBlockKey(key)
        lastKey.setFromBlockKey(key)
    }

    private fun finishBlock() {
        val builder = this.builder
        val encodedBlock = BlockUtil.encode(builder.build())
        meta.add(
            BlockMeta(
                offset = data.size(),
                firstKey = firstKey,
                lastKey = lastKey
            )
        )
        data += encodedBlock
        data += crcHash(encodedBlock).toU32ByteArray()

        // reset
        firstKey = BlockKey.empty()
        lastKey = BlockKey.empty()
        this.builder = BlockBuilder(blockSize)
    }

    // build SSTable and write it to given path
    private fun build(
        id: Int,
        blockCache: BlockCache?,
        path: Path
    ): SsTable {
        finishBlock()
        val buf = data
        val metaOffset = buf.size()
        BlockMetaUtil.encodeBlockMeta(meta, maxTimestamp, buf)
        buf += metaOffset.toU32ByteArray()

        // TODO: we will add bloom filter at the end of the Sstable
        val file = SsTableFile.create(path, buf)

        return SsTable(
            file = file,
            blockMeta = meta,
            blockMetaOffset = metaOffset,
            id = id,
            blockCache = blockCache,
            firstKey = meta.first().firstKey,
            lastKey = meta.last().lastKey,
            maxTs = 0
        )
    }

    fun buildForTest(path: Path): SsTable {
        return build(0, null, path)
    }
}
