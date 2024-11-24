package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.TimestampedKey
import org.github.seonwkim.common.farmHashFingerPrintU32
import org.github.seonwkim.common.toComparableByteArray
import org.github.seonwkim.lsm.bloom.Bloom
import org.github.seonwkim.lsm.bloom.BloomUtil
import org.github.seonwkim.lsm.sstable.Sstable
import org.github.seonwkim.lsm.sstable.SsTableBuilder
import org.github.seonwkim.lsm.sstable.SsTableFile
import kotlin.io.path.createTempDirectory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Day7 {

    companion object {
        private const val NUMBER_OF_KEYS = 100
    }

    @Test
    fun `test task1 bloom filter`() {
        val keyHashes = mutableListOf<UInt>()
        for (idx in 0 until NUMBER_OF_KEYS) {
            val key = createKey(idx)
            keyHashes.add(farmHashFingerPrintU32(key))
        }
        val bitsPerKey = BloomUtil.bloomBitsPerKey(keyHashes.size, 0.01)
        println("bits per key: $bitsPerKey")
        val bloom = Bloom.fromKeyHashes(keyHashes, bitsPerKey)
        println("bloom size: ${bloom.filter.size()}, hashFunctionsCount: ${bloom.hashFunctionsCount}")
        assertTrue { bloom.hashFunctionsCount < 30 }
        for (idx in 0 until NUMBER_OF_KEYS) {
            val key = createKey(idx)
            assertTrue { bloom.mayContain(farmHashFingerPrintU32(key)) }
        }

        var x = 0
        var count = 0
        for (idx in NUMBER_OF_KEYS..NUMBER_OF_KEYS * 10) {
            val key = createKey(idx)
            if (bloom.mayContain(farmHashFingerPrintU32(key))) {
                x += 1
            }
            count += 1
        }
        assertTrue("bloom filter isn't efficient enough") { x != count }
        assertTrue("bloom filter not taking effect") { x != 0 }
    }

    @Test
    fun `test task2 sst decode`() {
        val builder = SsTableBuilder(128)
        for (idx in 0 until NUMBER_OF_KEYS) {
            val key = createKey(idx)
            val value = createValue(idx)
            builder.add(TimestampedKey(key), value)
        }

        val dir = createTempDirectory("test_task2_sst_decode")
        val path = dir.resolve("1.sst")
        val sst1 = builder.buildForTest(path)
        val sst2 = Sstable.open(
            id = 0,
            blockCache = null,
            file = SsTableFile.open(path)
        )

        assertEquals(sst1.bloom?.hashFunctionsCount, sst2.bloom?.hashFunctionsCount)
        assertEquals(sst1.bloom?.filter, sst2.bloom?.filter)
    }

    private fun createKey(idx: Int): ComparableByteArray {
        return "key_%010d".format(idx * 5).toComparableByteArray()
    }

    private fun createValue(idx: Int): ComparableByteArray {
        return "value_%010d".format(idx).toComparableByteArray()
    }
}
