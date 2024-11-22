package org.github.seonwkim.common

import com.fasterxml.jackson.databind.ObjectMapper

object SerdeUtil {
    private val objectMapper = ObjectMapper()

    fun serialize(value: Any): List<Byte> {
        return objectMapper.writeValueAsBytes(value).toList()
    }

    fun <T> deserialize(bytes: ByteArray, clazz: Class<T>): T {
        return objectMapper.readValue(bytes, clazz)
    }
}
