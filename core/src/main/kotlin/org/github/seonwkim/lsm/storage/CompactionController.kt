package org.github.seonwkim.lsm.storage

sealed interface CompactionController {
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

class SimpleCompactionController(options: SimpleLeveledCompactionOptions) : CompactionController {
    override fun flushTol0(): Boolean {
        return true
    }
}

data object NoCompactionController : CompactionController {
    override fun flushTol0(): Boolean {
        return false
    }
}
