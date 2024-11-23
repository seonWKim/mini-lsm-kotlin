package org.github.seonwkim.lsm.storage

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import org.github.seonwkim.common.*
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = Flush::class, name = "Flush"),
    JsonSubTypes.Type(value = NewMemTable::class, name = "NewMemTable"),
    JsonSubTypes.Type(value = Compaction::class, name = "Compaction")
)
sealed interface ManifestRecord
data class Flush @JsonCreator constructor(
    @JsonProperty("sstId") val sstId: Int
) : ManifestRecord

data class NewMemTable @JsonCreator constructor(
    @JsonProperty("memTableId") val memTableId: Int
) : ManifestRecord

data class Compaction @JsonCreator constructor(
    @JsonProperty("task") val task: CompactionTask,
    @JsonProperty("output") val output: List<Int>
) : ManifestRecord

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
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC // TODO: is there a way to handle sync manually?
            ).use {
                File(path.toString())
            }

            return Manifest(file = MutexLock(value = file))
        }

        fun recover(path: Path): Pair<Manifest, List<ManifestRecord>> {
            val file = Files.newByteChannel(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC, // TODO: is there a way to handle sync manually?
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

    fun addRecord(record: ManifestRecord) {
        file.withWriteLock {
            val buf = SerdeUtil.serialize(record)
            val hash = crcHash(buf)

            it.appendBytes(buf.size.toLong().toU64ByteArray().getByteArray())
            it.appendBytes(buf)
            it.appendBytes(hash.toU32ByteArray().getByteArray())
        }
    }
}
