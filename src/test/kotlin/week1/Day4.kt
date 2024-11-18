package week1

import org.junit.jupiter.api.Test
import org.seonWKim.common.toComparableByteArray
import org.seonWKim.lsm.block.toBlockKey
import org.seonWKim.lsm.sstable.SsTableBuilder
import kotlin.io.path.createTempDirectory

class Day4 {

    @Test
    fun `test sst build single key`() {
        val builder = SsTableBuilder(16)
        builder.add("233".toBlockKey(), "233333".toComparableByteArray())
        val dir = createTempDirectory("test_Sst_build_single_key")
        builder.buildForTest(dir.resolve("1.sst"))
    }
}
