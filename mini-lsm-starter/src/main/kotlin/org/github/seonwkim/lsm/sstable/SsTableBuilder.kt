package org.github.seonwkim.lsm.sstable

import org.github.seonwkim.common.*
import org.github.seonwkim.lsm.block.BlockBuilder
import org.github.seonwkim.lsm.block.BlockMeta
import org.github.seonwkim.lsm.block.BlockMetaUtil
import org.github.seonwkim.lsm.block.BlockUtil
import org.github.seonwkim.lsm.bloom.Bloom
import org.github.seonwkim.lsm.bloom.BloomUtil
import java.nio.file.Path

class SsTableBuilder(
    private val blockSize: Int,
) {
    private var builder: BlockBuilder
    private var firstKey: ComparableByteArray
    private var lastKey: ComparableByteArray
    private val data: ComparableByteArray

    val meta: MutableList<BlockMeta>
    private val keyHashes: MutableList<UInt>

    // TODO(TIMESTAMP: add support for maxTimestamp)

    init {
        this.builder = BlockBuilder(blockSize)
        this.firstKey = ComparableByteArray.new()
        this.lastKey = ComparableByteArray.new()
        this.data = ComparableByteArray.new()
        this.meta = mutableListOf()
        this.keyHashes = mutableListOf()
    }

    fun add(key: ComparableByteArray, value: ComparableByteArray) {
        if (this.firstKey.isEmpty()) {
            firstKey.setFromByteArray(key)
        }

        // TODO(TIMESTAMP: should compare the key's timestamp and set the maxTimestamp if necessary)

        keyHashes.add(farmHashFingerPrintU32(key))

        if (builder.add(key, value)) {
            lastKey.setFromByteArray(key)
            return
        }

        // create a new block builder and append block data
        finishBlock()

        // add the key-value pair to the next block
        if (!builder.add(key, value)) {
            throw IllegalStateException("Unable to add key($key) and value($value) to builder")
        }
        firstKey.setFromByteArray(key)
        lastKey.setFromByteArray(key)
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
        firstKey = ComparableByteArray.new()
        lastKey = ComparableByteArray.new()
        this.builder = BlockBuilder(blockSize)
    }

    // build SSTable and write it to given path
    fun build(
        id: Int,
        blockCache: BlockCache?,
        path: Path
    ): Sstable {
        finishBlock()
        val buf = data
        val metaOffset = buf.size()
        BlockMetaUtil.encodeBlockMeta(meta, buf)
        buf += metaOffset.toU32ByteArray()

        val bloom = Bloom.fromKeyHashes(
            keys = keyHashes,
            bitsPerKey = BloomUtil.bloomBitsPerKey(keyHashes.size, 0.01)
        )
        val bloomOffset = buf.size()
        BloomUtil.encode(buf, bloom)
        buf += bloomOffset.toU32ByteArray()
        val file = SsTableFile.create(path, buf)

        return Sstable(
            file = file,
            blockMeta = meta,
            blockMetaOffset = metaOffset,
            id = id,
            blockCache = blockCache,
            firstKey = meta.first().firstKey,
            lastKey = meta.last().lastKey,
            bloom = bloom,
            // maxTs = 0
        )
    }

    fun buildForTest(path: Path): Sstable {
        return build(0, null, path)
    }

    fun estimatedSize(): Int {
        return data.size()
    }
}
