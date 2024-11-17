package week1

import org.example.common.toComparableByteArray
import org.example.lsm.block.BlockBuilder
import org.example.lsm.block.toBlockKey
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class Day3 {

    @Test
    fun `test block build single key`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("233".toBlockKey(), "233333".toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build full`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("11".toBlockKey(), "11".toComparableByteArray()) }
        assertFalse { builder.add("22".toBlockKey(), "22".toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build large 1`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("11".toBlockKey(), "1".repeat(100).toComparableByteArray()) }
        builder.build()
    }

    @Test
    fun `test block build large 2`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("11".toBlockKey(), "1".toComparableByteArray()) }
        assertFalse { builder.add("11".toBlockKey(), "1".repeat(100).toComparableByteArray()) }
        builder.build()
    }
}
