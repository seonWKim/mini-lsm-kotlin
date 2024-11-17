package org.example.lsm.block

import org.example.common.SIZEOF_U16
import org.example.common.toU16ByteArray
import org.example.common.toUInt

class Block(
    val data: List<Byte>,
    val offset: List<Byte>
) {
    companion object {
        fun decode(data: List<Byte>): Block {
            val entryOffsetLength = data.subList(data.size - SIZEOF_U16, data.size).toUInt()

            val dataEnd = data.size - SIZEOF_U16 - entryOffsetLength * SIZEOF_U16
            return Block(
                data = data.subList(0, dataEnd),
                offset = data.subList(dataEnd, data.size - SIZEOF_U16)
            )
        }
    }

    fun encode(): List<Byte> {
        val offsetLength = offset.size / SIZEOF_U16
        return data + offset + offsetLength.toU16ByteArray()
    }
}
