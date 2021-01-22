package entityLocker

import java.time.Duration
import java.util.WeakHashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock

class LockTimeoutException : Exception()
class LockAcquireTimeoutException : Exception()

/**
 * Ad hoc solution to store values in WeakHashMap under the hood of ConcurrentHashMap stripe locking.
 * ConcurrentHashMap can not store null values thus we can keep it empty by returning null as result of compute
 * function.
 */
class LocksRepo() {
    private val mapLock = ConcurrentHashMap<Any, ReentrantLock>()
    private val weakReferenceMap = WeakHashMap<Any, ReentrantLock>()

    fun findLock(entityId: Any): ReentrantLock {
        var result: ReentrantLock? = null
        mapLock.compute(entityId) { _, _ ->
            result = weakReferenceMap.computeIfAbsent(entityId) { ReentrantLock() }
            null
        }
        return result!!
    }
}

/**
 * Changes:
 *  1) Implemented global lock and lock escalation logic with ReentrantReadWriteLock
 *  2) Changed actual storage of data about locks to WeakHashMap proxied by ConcurrentHashMap for thread safety
 *
 * Utility class to handle blocking synchronisation based on entity identity.
 *
 * Attempts to lock amount of entities exceeding [escalationThreshold] by one thread will be escalated
 * to global lock.
 *
 * In order to avoid possible deadlocks use methods that try to acquire lock with timeout:
 * [tryWithLockById], [tryWithGlobalLock], [tryWithTimeout], [tryWithTimedGlobal]
 *
 * [scheduler] is responsible for handling locking timeout logic and should be provided by client
 *
 * @param escalationThreshold number of locked entities by one thread to escalate to global lock
 * @param scheduler service to schedule interruption of threads by timeout
 */
class EntityLocker(
    private val escalationThreshold: Int = 2,
    private val scheduler: ScheduledExecutorService,
    private val locksRepo: LocksRepo
) {
    private val globalLock = ReentrantReadWriteLock()
    private val globalShared = globalLock.readLock()
    private val globalExclusive = globalLock.writeLock()

    companion object {
        private val lockedEntityCounter = ThreadLocal.withInitial { HashMap<Any, Int>() }
        private val globalLockEntranceCounter = ThreadLocal.withInitial { 0 }

        fun increaseEntitiesCount(entityId: Any): Int {
            val lockedEntities = lockedEntityCounter.get()
            lockedEntities.merge(entityId, 1) { prev, one -> prev + one }!!
            return lockedEntities.filterValues { value: Int -> value > 0 }.count()
        }

        fun decreaseEntitiesCount(entityId: Any): Int {
            val lockedEntities = lockedEntityCounter.get()
            lockedEntities.merge(entityId, 1) { prev, one -> prev - one }!!
            return lockedEntities.filterValues { value: Int -> value > 0 }.count()
        }
    }

    /**
     * Executes the given [protectedCode] under lock corresponding to [entityId]
     * Prone to deadlocks especially in case of lock escalation. Prefer to use [tryWithLockById]
     */
    fun <T> withLockById(entityId: Any, protectedCode: () -> T): T {
        val entityLockCount = increaseEntitiesCount(entityId)
        return withGlobalShared(entityLockCount) {
            val entityLock = locksRepo.findLock(entityId)
            val result = try {
                entityLock.lock()
                try {
                    protectedCode()
                } finally {
                    entityLock.unlock()
                }
            } finally {
                decreaseEntitiesCount(entityId)
            }
            result
        }
    }

    /**
     * Executes the given [protectedCode] under global lock
     * Prone to deadlocks, prefer to use [tryWithGlobalLock]
     */
    fun <T> withGlobalLock(protectedCode: () -> T): T {
        return withEscalation { protectedCode() }
    }

    /**
     * Tries to execute the given [protectedCode] under lock corresponding to [entityId]
     * If it fails to acquire lock for period of [timeout] milliseconds [LockAcquireTimeoutException] will be thrown
     */
    fun <T> tryWithLockById(entityId: Any, timeout: Duration, protectedCode: () -> T): T {
        val isSharedLockAcquired = globalShared.tryLock(timeout.toMillis(), TimeUnit.MILLISECONDS)
        return if (isSharedLockAcquired) {
            increaseEntranceCount()
            val entitiesCount = increaseEntitiesCount(entityId)
            try {
                if (entitiesCount > escalationThreshold) {
                    tryWithEscalation(timeout, protectedCode)
                } else {
                    val entityLock = locksRepo.findLock(entityId)
                    tryWithLock(entityLock, timeout, protectedCode)
                }
            } finally {
                decreaseEntitiesCount(entityId)
                unlockGlobalShared()
            }
        } else {
            throw LockAcquireTimeoutException()
        }
    }

    /**
     * Tries to execute the given [protectedCode] under global lock
     * If it fails to acquire lock for period of [timeout] milliseconds [LockAcquireTimeoutException] will be thrown
     */
    fun <T> tryWithGlobalLock(timeout: Duration, protectedCode: () -> T): T {
        return tryWithEscalation(timeout, protectedCode)
    }

    /**
     * Executes the given [protectedCode] under lock corresponding to [entityId] but not longer than [lockingTimeout]
     * duration. If execution time of [protectedCode] exceeds [lockingTimeout]
     * thread will be interrupted and [LockTimeoutException] will be thrown
     * Prone to deadlocks especially in case of lock escalation. Prefer to use [tryWithTimeout]
     */
    fun <T> withTimeout(entityId: Any, lockingTimeout: Duration, protectedCode: () -> T): T {
        val count = globalLockEntranceCounter.get()
        globalShared.lock()
        globalLockEntranceCounter.set(count + 1)

        val currentThread = Thread.currentThread()

        return try {
            val futureInterrupt = scheduler.schedule(
                { currentThread.interrupt() },
                lockingTimeout.toMillis(), TimeUnit.MILLISECONDS
            )

            val result = if (increaseEntitiesCount(entityId) > escalationThreshold) {
                withTimedEscalation(lockingTimeout, currentThread) { protectedCode() }
            } else {
                val entityLock = locksRepo.findLock(entityId)
                lockWithTimeout(entityLock, lockingTimeout, Thread.currentThread(), protectedCode)
            }
            futureInterrupt.cancel(false)
            result
        } catch (ex: InterruptedException) {
            throw LockTimeoutException()
        } finally {
            decreaseEntitiesCount(entityId)
            unlockGlobalShared()
        }
    }

    /**
     * Executes the given [protectedCode] under global lock but not longer than [lockingTimeout]
     * duration. If execution time of [protectedCode] exceeds [lockingTimeout]
     * thread will be interrupted and [LockTimeoutException] will be thrown
     * Prone to deadlocks, prefer to use [tryWithTimedGlobal]
     */
    fun <T> withTimedGlobal(lockingTimeout: Duration, protectedCode: () -> T): T {
        return withTimedEscalation(lockingTimeout, Thread.currentThread(), protectedCode)
    }

    /**
     * Tries to execute the given [protectedCode] under lock corresponding to [entityId] but not longer than [lockingTimeout]
     * duration. If execution time of [protectedCode] exceeds [lockingTimeout]
     * thread will be interrupted and [LockTimeoutException] will be thrown
     * If it fails to acquire lock for period of [tryTimeout] milliseconds [LockAcquireTimeoutException] will be thrown
     */
    fun <T> tryWithTimeout(entityId: Any, lockingTimeout: Duration, tryTimeout: Duration, protectedCode: () -> T): T {
        val isSharedLockAcquired = globalShared.tryLock(tryTimeout.toMillis(), TimeUnit.MILLISECONDS)
        return if (isSharedLockAcquired) {
            val entitiesCount = increaseEntitiesCount(entityId)
            increaseEntranceCount()
            try {
                val currentThread = Thread.currentThread()
                val futureInterrupt =
                    scheduler.schedule({ currentThread.interrupt() }, lockingTimeout.toMillis(), TimeUnit.MILLISECONDS)
                val result = try {
                    if (entitiesCount > escalationThreshold) {
                        tryWithTimedEscalation(lockingTimeout, tryTimeout, currentThread, protectedCode)
                    } else {
                        val entityLock = locksRepo.findLock(entityId)
                        val isEntityLockAcquired = entityLock.tryLock(tryTimeout.toMillis(), TimeUnit.MILLISECONDS)
                        if (isEntityLockAcquired) {
                            lockWithTimeout(entityLock, lockingTimeout, currentThread, protectedCode)
                        } else {
                            throw LockAcquireTimeoutException()
                        }
                    }
                } finally {
                    decreaseEntitiesCount(entityId)
                }
                futureInterrupt.cancel(false)
                result
            } catch (ex: InterruptedException) {
                throw LockTimeoutException()
            } finally {
                unlockGlobalShared()
            }
        } else {
            throw LockAcquireTimeoutException()
        }
    }

    /**
     * Tries to execute the given [protectedCode] under global lock but not longer than [lockingTimeout]
     * duration. If execution time of [protectedCode] exceeds [lockingTimeout]
     * thread will be interrupted and [LockTimeoutException] will be thrown
     * If it fails to acquire lock for period of [tryTimeout] milliseconds [LockAcquireTimeoutException] will be thrown
     */
    fun <T> tryWithTimedGlobal(lockingTimeout: Duration, tryTimeout: Duration, protectedCode: () -> T):T {
        return tryWithTimedEscalation(lockingTimeout, tryTimeout, Thread.currentThread(), protectedCode)
    }

    private fun <T> tryWithLock(lock: Lock, timeoutMillis: Duration, protectedCode: () -> T): T {
        val isLockAcquired = lock.tryLock(timeoutMillis.toMillis(), TimeUnit.MILLISECONDS)
        if (isLockAcquired) {
            try {
                return protectedCode()
            } finally {
                lock.unlock()
            }
        } else {
            throw LockAcquireTimeoutException()
        }
    }

    private fun <T> lockWithTimeout(
        entityLock: ReentrantLock,
        lockingTimeout: Duration,
        currentThread: Thread,
        protectedCode: () -> T
    ): T {
        try {
            entityLock.lock()
            val futureInterrupt = scheduler.schedule({
                if (entityLock.isLocked)
                    currentThread.interrupt()
            }, lockingTimeout.toMillis(), TimeUnit.MILLISECONDS)

            val result = protectedCode()
            futureInterrupt.cancel(false)
            return result
        } catch (ex: InterruptedException) {
            throw LockTimeoutException()
        } finally {
            entityLock.unlock()
        }
    }


    private fun <T> withGlobalShared(entitiesLockCount: Int, action: () -> T): T {
        lockGlobalShared()
        return try {
            if (entitiesLockCount > escalationThreshold) {
                withEscalation { action() }
            } else {
                action()
            }
        } finally {
            unlockGlobalShared()
        }
    }

    private fun lockGlobalShared() {
        globalShared.lock()
        increaseEntranceCount()
    }

    private fun increaseEntranceCount(): Int {
        val count = globalLockEntranceCounter.get()
        globalLockEntranceCounter.set(count + 1)
        return count
    }

    private fun unlockGlobalShared() {
        val count = globalLockEntranceCounter.get()
        if (count > 0) {
            globalShared.unlock()
            globalLockEntranceCounter.set(count - 1)
        }
    }

    private fun <T> withEscalation(action: () -> T): T {
        val level = escalateLock()
        return try {
            action()
        } finally {
            deescalateLock(level)
        }
    }

    private fun <T> tryWithEscalation(timeout: Duration, action: () -> T): T {
        val entranceLevel = globalLockEntranceCounter.get()
        releaseShareLocks(entranceLevel)

        val isExclusiveAcquired = globalExclusive.tryLock(timeout.toMillis(), TimeUnit.MILLISECONDS)
        return if (isExclusiveAcquired) {
            try {
                action()
            } finally {
                deescalateLock(entranceLevel)
            }
        } else {
            restoreShareLocks(entranceLevel)
            throw LockAcquireTimeoutException()
        }
    }


    private fun <T> withTimedEscalation(
        lockingTimeout: Duration,
        currentThread: Thread,
        protectedCode: () -> T
    ): T {
        val entranceLevel = escalateLock()
        return try {
            val futureInterrupt =
                scheduler.schedule({ currentThread.interrupt() }, lockingTimeout.toMillis(), TimeUnit.MILLISECONDS)

            val result = protectedCode()
            futureInterrupt.cancel(false)
            result
        } catch (ex: InterruptedException) {
            throw LockTimeoutException()
        } finally {
            deescalateLock(entranceLevel)
        }
    }

    private fun <T> tryWithTimedEscalation(
        lockingTimeout: Duration,
        tryTimeout: Duration,
        currentThread: Thread,
        protectedCode: () -> T
    ): T {
        val entranceLevel = globalLockEntranceCounter.get()
        releaseShareLocks(entranceLevel)
        val isExclusiveAcquired = globalExclusive.tryLock(tryTimeout.toMillis(), TimeUnit.MILLISECONDS)
        return if (isExclusiveAcquired) {
            try {
                val futureInterrupt =
                    scheduler.schedule({ currentThread.interrupt() }, lockingTimeout.toMillis(), TimeUnit.MILLISECONDS)
                val result = protectedCode()
                futureInterrupt.cancel(false)
                result
            } catch (ex: InterruptedException) {
                throw LockTimeoutException()
            } finally {
                deescalateLock(entranceLevel)
            }
        } else {
            restoreShareLocks(entranceLevel)
            throw LockAcquireTimeoutException()
        }
    }


    private fun escalateLock(): Int {
        val entranceLevel = globalLockEntranceCounter.get()
        releaseShareLocks(entranceLevel)
        globalExclusive.lock()
        return entranceLevel
    }

    private fun deescalateLock(entranceLevel: Int) {
        restoreShareLocks(entranceLevel)
        globalExclusive.unlock()
    }

    private fun releaseShareLocks(entranceLevel: Int) {
        repeat(entranceLevel) {
            unlockGlobalShared()
        }
    }

    private fun restoreShareLocks(entranceLevel: Int) {
        repeat(entranceLevel) {
            lockGlobalShared()
        }
    }
}

