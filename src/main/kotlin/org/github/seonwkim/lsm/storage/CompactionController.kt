package org.github.seonwkim.lsm.storage

sealed interface CompactionController {
    fun flushTol0(): Boolean {
        return when (this) {
            is LeveledCompactionController,
            is SimpleCompactionController,
            is NoCompactionController -> true

            else -> false
        }
    }
}

class LeveledCompactionController(options: LeveledCompactionOptions) : CompactionController
class TieredCompactionController(options: TieredCompactionOptions) : CompactionController
class SimpleCompactionController(options: SimpleLeveledCompactionOptions) : CompactionController
data object NoCompactionController : CompactionController
