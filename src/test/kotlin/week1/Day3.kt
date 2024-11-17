package week1

import org.example.common.toComparableByteArray
import org.example.lsm.block.BlockBuilder
import kotlin.test.Test
import kotlin.test.assertTrue

class Day3 {

    @Test
    fun `test block build single key`() {
        val builder = BlockBuilder(16)
        assertTrue { builder.add("233".toComparableByteArray(), "233333".toComparableByteArray()) }
        builder.build()
    }
}
