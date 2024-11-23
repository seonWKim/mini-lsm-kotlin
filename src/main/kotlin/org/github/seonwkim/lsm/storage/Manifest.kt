package org.github.seonwkim.lsm.storage

import org.github.seonwkim.common.*
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

sealed interface ManifestRecord
data class Flush(val sstId: Int) : ManifestRecord
data class NewMemTable(val memTableId: Int) : ManifestRecord
data class Compaction(val task: CompactionTask, val output: List<Int>) : ManifestRecord

/**
 * Append only file which records all operations happened in the engine.
 * When lsm engine restarts, it will read the manifest file, reconstruct the state, and load the SSL files on the disk.
 */
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

        fun recover(path: Path): Pair<Manifest, List<ManifestRecord>> {
            val file = Files.newByteChannel(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.APPEND,
            ).use {
                File(path.toString())
            }

            val buf = Files.readAllBytes(path)
            val bufPrt = ByteBuffer.wrap(buf)
            val records = mutableListOf<ManifestRecord>()

            while (bufPrt.hasRemaining()) {
                val len = bufPrt.getLong()
                val slice = ByteArray(len.toInt())
                bufPrt.get(slice)
                val json = SerdeUtil.deserialize<ManifestRecord>(slice)
                val checksum = bufPrt.getInt()
                val crc32 = crcHash(slice)
                if (checksum != crc32) {
                    throw IllegalStateException("checksum mismatch!")
                }
                records.add(json)
            }

            return Pair(Manifest(MutexLock(file)), records)
        }
    }

    fun addRecordWhenInit(record: ManifestRecord) {
        val file = file.withWriteLock {
            val buf = SerdeUtil.serialize(record)
            val hash = crcHash(buf)

            it.appendBytes(buf.size.toLong().toU64ByteArray().getByteArray())
            it.appendBytes(buf)
            it.appendBytes(hash.toU32ByteArray().getByteArray())
        }
    }
}
