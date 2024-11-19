package week1

import org.junit.jupiter.api.Test
import org.seonWKim.common.toComparableByteArray
import org.seonWKim.lsm.block.toBlockKey
import org.seonWKim.lsm.sstable.SsTableBuilder
import kotlin.io.path.createTempDirectory
import kotlin.test.assertTrue

class Day4 {

    @Test
    fun `test sst build single key`() {
        val builder = SsTableBuilder(16)
        builder.add("233".toBlockKey(), "233333".toComparableByteArray())
        val dir = createTempDirectory("test_Sst_build_single_key")
        builder.buildForTest(dir.resolve("1.sst"))
    }

    @Test
    fun `test sst build two blocks`() {
        val builder = SsTableBuilder(16)
        builder.add("11".toBlockKey(), "11".toComparableByteArray())
        builder.add("22".toBlockKey(), "22".toComparableByteArray())
        builder.add("33".toBlockKey(), "11".toComparableByteArray())
        builder.add("44".toBlockKey(), "22".toComparableByteArray())
        builder.add("55".toBlockKey(), "11".toComparableByteArray())
        builder.add("66".toBlockKey(), "22".toComparableByteArray())
        assertTrue { builder.meta.size >= 2 }
        val dir = createTempDirectory("test_set_build_two_blocks")
        builder.buildForTest(dir.resolve("1.sst"))
    }
}
