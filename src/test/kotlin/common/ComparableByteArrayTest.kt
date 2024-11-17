package common

import org.seonWKim.common.toComparableByteArray
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ComparableByteArrayTest {

    @Test
    fun `ComparableByteArray should compare inner UByteArray appropriately`() {
        val arr1 = "abc".toComparableByteArray()
        val arr2 = "abcd".toComparableByteArray()
        assertTrue { arr1 < arr2 }

        val arr3 = "b".toComparableByteArray()
        val arr4 = "a".toComparableByteArray()
        assertTrue { arr3 > arr4 }

        val arr5 = "aa".toComparableByteArray()
        val arr6 = "aa".toComparableByteArray()
        assertTrue { arr5.compareTo(arr6) == 0 }
    }

    @Test
    fun `ComparableByteArray should compare inner UByteArray appropriately 2`() {
        val arr1 = "abcd".toComparableByteArray()
        val arr2 = "abc".toComparableByteArray()
        val arr3 = "cd".toComparableByteArray()
        val sortedList = listOf(arr1, arr2, arr3).sorted()
        assertEquals(sortedList[0], arr2)
        assertEquals(sortedList[1], arr1)
        assertEquals(sortedList[2], arr3)
    }
}
