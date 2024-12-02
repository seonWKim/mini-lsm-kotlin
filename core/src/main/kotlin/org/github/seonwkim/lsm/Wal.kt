package org.github.seonwkim.lsm

import mu.KotlinLogging
import org.github.seonwkim.common.*
import org.github.seonwkim.common.lock.MutexLock
import org.github.seonwkim.lsm.memtable.MemtableValue
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentSkipListMap

class Wal(private val file: MutexLock<File>) {

    companion object {
        private val log = KotlinLogging.logger { }

        fun create(path: Path): Wal {
            return Wal(
                file = Files.newByteChannel(
                    path,
                    StandardOpenOption.READ,
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.SYNC // TODO: is there a better way to handle sync?
                ).use {
                    File(path.toString())
                }.let { MutexLock(it) }
            )
        }

        fun recover(path: Path, map: ConcurrentSkipListMap<TimestampedKey, MemtableValue>): Wal {
            val file = Files.newByteChannel(
                path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.SYNC
            ).use {
                File(path.toString())
            }

            val buf = Files.readAllBytes(path)
            val bufPrt = ByteBuffer.wrap(buf)

            while (bufPrt.hasRemaining()) {
                val keyLenSlice = ByteArray(SIZE_OF_U16_IN_BYTE)
                bufPrt.get(keyLenSlice)
                val keyLen = keyLenSlice.toComparableByteArray().toU16Int()

                val keySlice = ByteArray(keyLen)
                bufPrt.get(keySlice)
                val key = keySlice.toComparableByteArray()

                val valueLenSlice = ByteArray(SIZE_OF_U16_IN_BYTE)
                bufPrt.get(valueLenSlice)
                val valueLen = valueLenSlice.toComparableByteArray().toU16Int()

                val valueSlice = ByteArray(valueLen)
                bufPrt.get(valueSlice)
                val value = valueSlice.toComparableByteArray()

                val checksumSlice = ByteArray(SIZE_OF_U32_IN_BYTE)
                bufPrt.get(checksumSlice)
                val checksum = checksumSlice.toComparableByteArray().toU32Int()

                val combined = keyLenSlice + keySlice + valueLenSlice + valueSlice
                val calculatedChecksum = crcHash(combined)

                if (checksum == calculatedChecksum) {
                    // TODO: for now, set the key's timestamp to 0
                    val timestampedKey = TimestampedKey(key)
                    map[timestampedKey] = MemtableValue(value)
                    log.debug { "[$timestampedKey, $value] recovered from WAL"}
                } else {
                    throw IllegalStateException("Checksum mismatch")
                }
            }

            return Wal(
                file = MutexLock(file)
            )
        }
    }

    fun put(key: TimestampedKey, value: MemtableValue) {
        this.file.withWriteLock {
            val buf = ComparableByteArray.new()

            buf += key.size().toU16ByteArray()
            buf += key.bytes
            buf += value.size().toU16ByteArray()
            buf += value.value

            buf += crcHash(buf).toU32ByteArray()
            it.appendBytes(buf.getByteArray())
        }
    }

    fun sync() {
        // TODO: enhance buffering and sync logic
    }
}

fun walPath(path: Path, id: Int): Path {
    return path.resolve("%05d.wal".format(id))
}
