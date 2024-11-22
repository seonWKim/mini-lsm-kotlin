package org.github.seonwkim.minilsm.week1

import org.github.seonwkim.common.ComparableByteArray
import org.github.seonwkim.common.farmHashFingerPrint64
import org.github.seonwkim.common.toComparableByteArray
import kotlin.test.Test

class Day7 {

    companion object {
        private const val NUMBER_OF_KEYS = 100
    }

    @Test
    fun `test task1 bloom filter`() {
        val keyHashes = mutableListOf<Int>()
        for (idx in 0 until NUMBER_OF_KEYS) {
            val key = createKey(idx)
            keyHashes.add(farmHashFingerPrint64(key))
        }
    }

    private fun createKey(idx: Int): ComparableByteArray {
        return "key_%010d".format(idx * 5).toComparableByteArray()
    }

    private fun createValue(idx: Int): ComparableByteArray {
        return "value_%010d".format(idx).toComparableByteArray()
    }
}
