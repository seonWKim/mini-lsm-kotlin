package org.seonWKim.lsm.sstable

import org.seonWKim.common.ComparableByteArray
import java.io.File
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path

/**
 * TODO: the project should remove unnecessary conversion between ByteArray and
 *       let's decide which one to use in the future
 */
class SsTableFile( // original file name was FileObject
    var file: File?,
    val size: Long
) {
    companion object {
        fun create(path: Path, data: ComparableByteArray): SsTableFile {
            val file = path.toFile()
            Files.write(path, data.getByteArray())
            val size = Files.size(path)
            return SsTableFile(file, size)
        }
    }

    fun read(offset: Long, length: Int): ComparableByteArray {
        val fileChannel = file!!.inputStream().channel
        val buffer = ByteBuffer.allocate(length)
        fileChannel.use {
            it.position(offset)
            it.read(buffer)
        }
        buffer.flip()
        val byteArray = ByteArray(buffer.remaining())
        buffer.get(byteArray)
        return ComparableByteArray(byteArray.toList())
    }
}
