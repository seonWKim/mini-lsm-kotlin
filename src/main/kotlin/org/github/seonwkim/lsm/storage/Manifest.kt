package org.github.seonwkim.lsm.storage

import org.github.seonwkim.common.MutexLock
import org.github.seonwkim.common.RwLock
import org.github.seonwkim.common.SerdeUtil
import org.github.seonwkim.common.crcHash
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

sealed interface ManifestRecord {
    data class Flush(val size: Int) : ManifestRecord
    data class NewMemTable(val size: Int) : ManifestRecord
    data class Compaction(val task: CompactionTask, val output: List<Int>)
}

class Manifest(
    val file: RwLock<File>
) {
    companion object {
        fun create(path: Path): Manifest {
            val file = Files.newByteChannel(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.WRITE
            ).use {
                File(path.toString())
            }

            return Manifest(file = MutexLock(value = file))
        }
    }

    // TODO: implement
    fun addRecordWhenInit(record: ManifestRecord) {
        val file = file.withWriteLock {
            val buf = SerdeUtil.serialize(record)
            val hash = crcHash(buf)

            val lengthBytes = ByteBuffer.allocate(Long.SIZE_BYTES).putLong(buf.size.toLong()).array()
            val hashBytes = ByteBuffer.allocate(Int.SIZE_BYTES).putInt(hash).array()
            // it.appendBytes()
        }
    }
}
