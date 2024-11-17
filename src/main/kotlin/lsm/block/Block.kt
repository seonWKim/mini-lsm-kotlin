package org.seonWKim.lsm.block

import org.seonWKim.common.SIZE_OF_U16_IN_BYTE
import org.seonWKim.common.toU16ByteArray
import org.seonWKim.common.toUInt

class Block(
    val data: List<Byte>,
    val offsets: List<Byte>
) {
    companion object {
        fun decode(data: List<Byte>): Block {
            val entryOffsetLength = data.subList(data.size - SIZE_OF_U16_IN_BYTE, data.size).toUInt()

            val dataEnd = data.size - SIZE_OF_U16_IN_BYTE - entryOffsetLength * SIZE_OF_U16_IN_BYTE
            return Block(
                data = data.subList(0, dataEnd),
                offsets = data.subList(dataEnd, data.size - SIZE_OF_U16_IN_BYTE)
            )
        }
    }

    fun encode(): List<Byte> {
        val offsetLength = offsets.size / SIZE_OF_U16_IN_BYTE
        return data + offsets + offsetLength.toU16ByteArray()
    }
}
