package org.github.seonwkim.common

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TimestampedByteArrayTest {

    @Test
    fun `TimestampedByteArray should compare inner UByteArray appropriately`() {
        val arr1 = "abc".toTimestampedByteArrayWithoutTs()
        val arr2 = "abcd".toTimestampedByteArrayWithoutTs()
        assertTrue { arr1 < arr2 }

        val arr3 = "b".toTimestampedByteArrayWithoutTs()
        val arr4 = "a".toTimestampedByteArrayWithoutTs()
        assertTrue { arr3 > arr4 }

        val arr5 = "aa".toTimestampedByteArrayWithoutTs()
        val arr6 = "aa".toTimestampedByteArrayWithoutTs()
        assertTrue { arr5.compareTo(arr6) == 0 }

        val arr7 = "aa".toTimestampedByteArray(timestamp = 1000)
        val arr8 = "aa".toTimestampedByteArray(timestamp = 2000)
        assertTrue { arr7 > arr8 }
    }

    @Test
    fun `TimestampedByteArray should compare inner UByteArray appropriately 2`() {
        val arr1 = "abcd".toTimestampedByteArrayWithoutTs()
        val arr2 = "abc".toTimestampedByteArrayWithoutTs()
        val arr3 = "cd".toTimestampedByteArrayWithoutTs()
        val sortedList = listOf(arr1, arr2, arr3).sorted()
        assertEquals(sortedList[0], arr2)
        assertEquals(sortedList[1], arr1)
        assertEquals(sortedList[2], arr3)
    }
}
