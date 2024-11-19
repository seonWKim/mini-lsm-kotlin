package org.seonWKim.lsm.sstable

import java.io.File
import java.nio.file.Files
import java.nio.file.Path

// The original name was FileObject
class SsTableFile(
    var file: File?,
    val size: Long
) {
    companion object {
        fun create(path: Path, data: List<Byte>): SsTableFile {
            val file = path.toFile()
            Files.write(path, data.toByteArray())
            val size = Files.size(path)
            return SsTableFile(file, size)
        }
    }
}
