package org.github.seonwkim.common

import org.github.seonwkim.common.lock.PriorityAwareLock
import org.junit.jupiter.api.assertThrows
import kotlin.test.Test
import kotlin.test.assertTrue

class LockTest {

    @Test
    fun `PriorityCheckLock test`() {
        val higherPriorityLock = PriorityAwareLock("testValue", 1)
        val lowerPriorityLock = PriorityAwareLock("testValue", 0)

        val result = higherPriorityLock.withWriteLock {
            lowerPriorityLock.withWriteLock { value ->
                assertTrue { value == "testValue" }
                "success"
            }
        }
        assertTrue { result == "success" }

        assertThrows<Error> {
            lowerPriorityLock.withWriteLock {
                higherPriorityLock.withWriteLock { value ->
                    // should not reach here
                }
            }
        }
    }
}
