package org.github.seonwkim.lsm.storage

sealed interface CompactionController {
    class LeveledCompactionController(options: LeveledCompactionOptions) : CompactionController
    class TieredCompactionController(options: TieredCompactionOptions) : CompactionController
    class Simple(options: SimpleLeveledCompactionOptions) : CompactionController
    data object NoCompaction : CompactionController
}
