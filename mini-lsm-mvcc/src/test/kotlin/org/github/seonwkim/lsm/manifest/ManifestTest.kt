package org.github.seonwkim.lsm.manifest

import org.github.seonwkim.lsm.compaction.task.ForceFullCompactionTask
import org.github.seonwkim.lsm.compaction.task.LeveledCompactionTask
import org.github.seonwkim.lsm.compaction.task.SimpleLeveledCompactionTask
import org.junit.jupiter.api.Test
import kotlin.io.path.createTempDirectory
import kotlin.test.assertEquals

class ManifestTest {

    @Test
    fun `create manifest file and recover`() {
        val dir = createTempDirectory()
        val manifestPath = dir.resolve("MANIFEST")
        val manifest = Manifest.create(manifestPath)

        val record0 = NewMemTableRecord(1)
        val record1 = FlushRecord(2)
        val record2 = CompactionRecord(
            task = LeveledCompactionTask(
                upperLevel = 10,
                upperLevelSstIds = listOf(1),
                lowerLevel = 1,
                lowerLevelSstIds = listOf(2),
                lowerLevelBottomLevel = true
            ),
            output = listOf(1, 2, 3)
        )
        val record3 = CompactionRecord(
            task = org.github.seonwkim.lsm.compaction.task.TieredCompactionTask(
                tiers = emptyList(),
                bottomTierIncluded = false
            ),
            output = listOf(1, 2, 3)
        )
        val record4 = CompactionRecord(
            task = SimpleLeveledCompactionTask(
                upperLevel = 10,
                upperLevelSstIds = listOf(1),
                lowerLevel = 1,
                lowerLevelSstIds = listOf(2),
                lowerLevelBottomLevel = true
            ),
            output = listOf(1, 2, 3)
        )
        val record5 = CompactionRecord(
            task = ForceFullCompactionTask(
                l0Sstables = listOf(1, 2),
                l1Sstables = listOf(3, 4)
            ),
            output = listOf(1, 2, 3)
        )

        manifest.addRecord(record0)
        manifest.addRecord(record1)
        manifest.addRecord(record2)
        manifest.addRecord(record3)
        manifest.addRecord(record4)
        manifest.addRecord(record5)

        val recoveredManifestRecords = Manifest.recover(manifestPath).second
        assertEquals(recoveredManifestRecords[0], record0)
        assertEquals(recoveredManifestRecords[1], record1)
        assertEquals(recoveredManifestRecords[2], record2)
        assertEquals(recoveredManifestRecords[3], record3)
        assertEquals(recoveredManifestRecords[4], record4)
    }
}
