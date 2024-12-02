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
    private var firstKey: TimestampedKey
    private var lastKey: TimestampedKey
    private val data: ComparableByteArray

    val meta: MutableList<BlockMeta>
    private val keyHashes: MutableList<UInt>
    private var maxTimestamp: Long

    init {
        this.builder = BlockBuilder(blockSize)
        this.firstKey = TimestampedKey.empty()
        this.lastKey = TimestampedKey.empty()
        this.data = ComparableByteArray.new()
        this.meta = mutableListOf()
        this.keyHashes = mutableListOf()
        this.maxTimestamp = 0L
    }

    fun add(key: TimestampedKey, value: ComparableByteArray) {
        if (this.firstKey.isEmpty()) {
            firstKey.setFromBlockKey(key)
        }

        if (key.timestamp() > maxTimestamp) {
            maxTimestamp = key.timestamp()
        }
        keyHashes.add(farmHashFingerPrintU32(key.bytes))

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
        firstKey = TimestampedKey.empty()
        lastKey = TimestampedKey.empty()
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
        BlockMetaUtil.encodeBlockMeta(meta, maxTimestamp, buf)
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
            maxTs = 0
        )
    }

    fun buildForTest(path: Path): Sstable {
        return build(0, null, path)
    }

    fun estimatedSize(): Int {
        return data.size()
    }
}
