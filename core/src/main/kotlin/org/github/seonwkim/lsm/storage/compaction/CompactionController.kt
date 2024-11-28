package org.github.seonwkim.lsm.storage.compaction

sealed interface CompactionController {
    companion object {
        fun createCompactionController(option: CompactionOptions): CompactionController {
            return when (option) {
                is NoCompaction -> NoCompactionController
                is SimpleLeveledCompactionOptions -> SimpleLeveledCompactionController(option)
                is LeveledCompactionOptions -> LeveledCompactionController(option)
                is TieredCompactionOptions -> TieredCompactionController(option)
            }
        }
    }

    fun flushTol0(): Boolean
}

class LeveledCompactionController(options: LeveledCompactionOptions) : CompactionController {
    override fun flushTol0(): Boolean {
        return true
    }
}

class TieredCompactionController(options: TieredCompactionOptions) : CompactionController {
    override fun flushTol0(): Boolean {
        return true
    }
}

class SimpleLeveledCompactionController(options: SimpleLeveledCompactionOptions) : CompactionController {
    override fun flushTol0(): Boolean {
        return true
    }
}

data object NoCompactionController : CompactionController {
    override fun flushTol0(): Boolean {
        return false
    }
}
