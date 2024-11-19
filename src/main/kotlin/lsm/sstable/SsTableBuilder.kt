package org.seonWKim.lsm.sstable

import org.seonWKim.common.*
import org.seonWKim.lsm.block.BlockBuilder
import org.seonWKim.lsm.block.BlockKey
import org.seonWKim.lsm.block.BlockMeta
import java.nio.file.Path

class SsTableBuilder(
    private val blockSize: Int,
) {
    private var builder: BlockBuilder
    private var firstKey: BlockKey
    private var lastKey: BlockKey
    private val data: MutableList<Byte>

    val meta: MutableList<BlockMeta>
    private val keyHashes: MutableList<Int>
    private var maxTimestamp: Long

    init {
        this.builder = BlockBuilder(blockSize)
        this.firstKey = BlockKey.empty()
        this.lastKey = BlockKey.empty()
        this.data = mutableListOf()
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
        keyHashes.add(murmurHash(value.getByteArray()))

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
        val encodedBlock = builder.build().encode()
        meta.add(
            BlockMeta(
                offset = data.size,
                firstKey = firstKey,
                lastKey = lastKey
            )
        )
        data.addAll(encodedBlock)
        data.addAll(crcHash(encodedBlock).toU32ByteArray())

        // reset
        firstKey = BlockKey.empty()
        lastKey = BlockKey.empty()
        this.builder = BlockBuilder(blockSize)
    }

    // build SSTable and write it to given path
    fun build(
        id: Int,
        blockCache: BlockCache?,
        path: Path
    ): SsTable {
        finishBlock()
        val buf = data
        val metaOffset = buf.size
        encodeBlockMeta(meta, maxTimestamp, buf)
        buf.addAll(metaOffset.toU32ByteArray())

        // TODO: bloom filter
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

    private fun encodeBlockMeta(blockMeta: List<BlockMeta>, maxTimestamp: Long, buf: MutableList<Byte>) {
        val originalLength = buf.size
        buf.addAll(blockMeta.size.toU32ByteArray())
        for (meta in blockMeta) {
            buf.addAll(meta.offset.toU32ByteArray())
            buf.addAll(meta.firstKey.size().toU16ByteArray())
            buf.addAll(meta.firstKey.key.array)
            buf.addAll(meta.firstKey.timestamp().toU64ByteArray())
            buf.addAll(meta.lastKey.size().toU16ByteArray())
            buf.addAll(meta.lastKey.key.array)
            buf.addAll(meta.lastKey.timestamp().toU64ByteArray())
        }
        buf.addAll(maxTimestamp.toU64ByteArray())
        buf.addAll(crcHash(buf.slice(originalLength + 4..<buf.size)).toU32ByteArray())
    }


    fun buildForTest(path: Path): SsTable {
        return build(0, null, path)
    }
}
