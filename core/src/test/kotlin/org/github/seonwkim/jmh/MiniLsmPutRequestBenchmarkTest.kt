package org.github.seonwkim.jmh

import mu.KotlinLogging
import org.github.seonwkim.lsm.LsmStorageOptions
import org.github.seonwkim.lsm.MiniLsm
import org.github.seonwkim.lsm.compaction.option.NoCompaction
import org.openjdk.jmh.annotations.*
import java.util.concurrent.TimeUnit
import kotlin.io.path.createTempDirectory
import kotlin.random.Random


@Fork(1)
@State(Scope.Benchmark)
open class MiniLsmPutRequestBenchmarkTest {

    private val log = KotlinLogging.logger { }
    lateinit var lsm: MiniLsm

    @Setup
    fun setUp() {
        log.info { "Initializing LSM!!" }
        val path = createTempDirectory("minilsm_benchmark").toAbsolutePath().resolve("benchmark-output")
        lsm = MiniLsm.open(
            path = path,
            options = LsmStorageOptions(
                blockSize = 4096,
                targetSstSize = 2 shl 20,
                numMemTableLimit = 10,
                enableWal = false,
                compactionOptions = NoCompaction,
            )
        )
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @Warmup(time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 5, time = 30, timeUnit = TimeUnit.SECONDS)
    @Threads(10)
    fun putRequest(state: MiniLsmPutRequestBenchmarkTest) {
        val key = Random.nextBytes(16).joinToString("") { "%02x".format(it) }
        val value = Random.nextBytes(32).joinToString("") { "%02x".format(it) }
        state.lsm.put(key, value)
    }
}
