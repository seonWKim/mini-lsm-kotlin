package org.github.seonwkim.common

import org.junit.jupiter.api.Assertions.assertEquals
import kotlin.test.Test

class ExtensionsTest {
    @Test
    fun `test rotateLeft`() {
        val int1 = 0b00000000000000000000000000000001
        val actual1 = int1.rotateLeft(3)
        assertEquals(0b00000000000000000000000000001000, actual1)

        val int2 = 0b01100000000000000000000000000000
        val actual2 = int2.rotateLeft(3)
        assertEquals(0b00000000000000000000000000000011, actual2)

        val int3 = 0b00000000000000000000000000000111
        val actual3 = int3.rotateLeft(0)
        assertEquals(0b00000000000000000000000000000111, actual3)
    }
}
