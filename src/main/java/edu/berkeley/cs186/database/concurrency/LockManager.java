package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains a record of which transactions hold which locks on which resources,
 * and handles waiting logic. Typically, you should not use LockManager directly; instead, you should
 * use methods of LockContext to acquire, release, promote, and escalate locks.
 *
 * LockManager is primarily concerned with the mapping between transactions, resources, and locks,
 * and does not concern itself with multigranularity, which is handled by LockContext instead.
 *
 * Each resource managed by LockManager has its own queue of LockRequest objects representing lock acquisition
 * (or promotion/acquisition-release) requests that could not be satisfied at the time. This queue should be processed
 * whenever a lock on the resource is released, starting with the first request and proceeding in order until a request
 * cannot be satisfied. Requests removed from the queue should be treated as if the transaction had just made the request
 * immediately after the resource was released (e.g., if T1 requested an X lock on db, T1 should be granted the X lock on db
 * and transitioned to an unblocked state via Transaction#unblock).
 *
 * This means that when the queue is processed:
 *    Queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue.
 */
public class LockManager {
    // transactionLocks maps transaction numbers to lists of locks held by those transactions.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries maps resource names to ResourceEntry objects.
    // A ResourceEntry object includes a list of locks on the object and a queue of requests for the resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // ResourceEntry includes the current list of locks granted on the resource and a queue of lock requests for the resource.
    private class ResourceEntry {
        // The current list of locks granted on the resource.
        List<Lock> locks = new ArrayList<>();
        // The queue of lock requests for the resource that have not yet been satisfied.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below is a list of suggested helper methods.
        // You may modify, delete, or ignore these method signatures.

        /**
         * Checks if `lockType` is compatible with existing locks, allowing for conflicts with locks held by the transaction with ID `except`.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            for (Lock lock : locks) {
                if (lock.transactionNum != except && !LockType.compatible(lock.lockType, lockType)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Grants `lock` to a transaction. This method assumes the lock is compatible.
         * If the transaction already holds a lock on the resource, it updates the lock on the resource.
         */
        public void grantOrUpdateLock(Lock lock) {
            for (int i = 0; i < locks.size(); i++) {
                if (locks.get(i).transactionNum == lock.transactionNum) {
                    locks.set(i, lock);
                    return;
                }
            }
            locks.add(lock);
            transactionLocks.put(lock.transactionNum, locks);
        }

        /**
         * Releases `lock` and processes the queue. This method assumes the lock was previously granted.
         */
        public void releaseLock(Lock lock) {
            locks.remove(lock);
            transactionLocks.remove(lock.transactionNum);
            processQueue();
        }

        /**
         * Adds `request` to the front or back of the queue.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            if (addFront) {
                waitingQueue.addFirst(request);
            } else {
                waitingQueue.addLast(request);
            }
        }

        /**
         * Processes the queue from front to back, granting locks to requests until a request cannot be granted.
         * When a request is fully granted, the transaction making the request can be unblocked.
         */
        private void processQueue() {
            while (!waitingQueue.isEmpty()) {
                LockRequest request = waitingQueue.peek();
                if (checkCompatible(request.lock.lockType, request.transaction.getTransNum())) {
                    waitingQueue.poll();
                    grantOrUpdateLock(request.lock);
                    request.transaction.unblock();
                } else {
                    break;
                }
            }
        }

        /**
         * Retrieves the lock type held by `transaction` on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            for (Lock lock : locks) {
                if (lock.transactionNum == transaction) {
                    return lock.lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // Do not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to retrieve the resourceEntry corresponding to `name`.
     * If the entry does not yet exist, a new (empty) resourceEntry is inserted into the map.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquires a `lockType` lock for `transaction` on `name`, and releases all locks held by `releaseNames` after the lock is acquired.
     *
     * Error checks must be performed before acquiring or releasing locks. If the new lock is incompatible with other transactions' locks on the resource,
     * the transaction is blocked, and the request is placed at the front of the resource's queue.
     *
     * Locks in `releaseNames` should only be released after the requested lock is acquired. Their queues must also be processed.
     *
     * Acquiring and then releasing a previous lock on `name` should not change the acquisition time of the lock on `name`.
     * For example, if a transaction acquires locks in the order S(A), X(B), X(A) and then releases S(A),
     * the lock on A should be considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already holds a lock of type `lockType` on `name` and it has not been released.
     * @throws NoLockHeldException if `transaction` does not hold a lock on one or more of the names in `releaseNames`.
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            if (entry.getTransactionLockType(transaction.getTransNum()) == lockType) {
                throw new DuplicateLockRequestException("Transaction already holds a lock of type " + lockType + " on resource " + name);
            }
            if (!entry.checkCompatible(lockType, transaction.getTransNum())) {
                shouldBlock = true;
                entry.addToQueue(new LockRequest(transaction, new Lock(name, lockType, transaction.getTransNum())), true);
            } else {
                for (ResourceName resourceName : releaseNames) {
                    ResourceEntry releaseEntry = getResourceEntry(resourceName);
                    LockType heldType = releaseEntry.getTransactionLockType(transaction.getTransNum());
                    if (heldType == LockType.NL) {
                        throw new NoLockHeldException("Transaction does not hold any lock on resource " + resourceName);
                    }
                    releaseEntry.releaseLock(new Lock(resourceName, heldType, transaction.getTransNum()));
                }
                entry.grantOrUpdateLock(new Lock(name, lockType, transaction.getTransNum()));
            }
        }
        if (shouldBlock) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Acquires a `lockType` lock for `transaction` on `name`.
     *
     * Error checks must be performed before acquiring the lock. If the new lock is incompatible with other transactions' locks on the resource,
     * or if there are other transactions waiting for the resource, the transaction is blocked, and the request is placed at the back of the queue for `name`.
     *
     * @throws DuplicateLockRequestException if `transaction` already holds a lock of type `lockType` on `name`.
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            if (entry.getTransactionLockType(transaction.getTransNum()) == lockType) {
                throw new DuplicateLockRequestException("Transaction already holds a lock of type " + lockType + " on resource " + name);
            }
            if (!entry.checkCompatible(lockType, transaction.getTransNum())) {
                shouldBlock = true;
                entry.addToQueue(new LockRequest(transaction, new Lock(name, lockType, transaction.getTransNum())), false);
            } else {
                entry.grantOrUpdateLock(new Lock(name, lockType, transaction.getTransNum()));
            }
        }
        if (shouldBlock) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Releases the lock held by `transaction` on `name`. Error checks must be performed before releasing the lock.
     *
     * After this call, the queue for the resource name must be processed. If there are requests in the queue that require the lock to be released,
     * those locks must also be released, and their queues processed.
     *
     * @throws NoLockHeldException if `transaction` does not hold a lock on `name`.
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            LockType heldType = entry.getTransactionLockType(transaction.getTransNum());
            if (heldType == LockType.NL) {
                throw new NoLockHeldException("Transaction does not hold any lock on resource " + name);
            }
            entry.releaseLock(new Lock(name, heldType, transaction.getTransNum()));
        }
    }

    /**
     * Promotes the lock held by `transaction` on `name` to `newLockType` (i.e., changes the current lock type of the transaction on `name` to `newLockType`).
     *
     * Error checks must be performed before changing the lock. If the new lock is incompatible with other transactions' locks on the resource,
     * the transaction is blocked, and the request is placed at the front of the queue for the resource.
     *
     * Lock promotion should not change the acquisition time of the lock. For example, if a transaction acquires locks in the order S(A), X(B), and then promotes X(A),
     * the lock on A should be considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already holds a lock of type `newLockType` on `name`.
     * @throws NoLockHeldException if `transaction` does not hold any lock on `name`.
     * @throws InvalidLockException if the requested lock type is not a valid promotion. A promotion from lock type A to B is only valid if B is substitutable for A and B is not the same as A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        boolean shouldBlock = false;
        synchronized (this) {
            ResourceEntry entry = getResourceEntry(name);
            LockType currentType = entry.getTransactionLockType(transaction.getTransNum());
            if (currentType == newLockType) {
                throw new DuplicateLockRequestException("Transaction already has a " + newLockType + " lock on resource " + name);
            }
            if (currentType == LockType.NL) {
                throw new NoLockHeldException("Transaction does not hold any lock on resource " + name);
            }
            if (!LockType.substitutable(newLockType, currentType)) {
                throw new InvalidLockException("Requested lock type is not a valid promotion");
            }
            if (!entry.checkCompatible(newLockType, transaction.getTransNum())) {
                shouldBlock = true;
                entry.addToQueue(new LockRequest(transaction, new Lock(name, newLockType, transaction.getTransNum())), true);
            } else {
                entry.grantOrUpdateLock(new Lock(name, newLockType, transaction.getTransNum()));
            }
        }
        if (shouldBlock) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Returns the lock type held by `transaction` on `name`, or NL if no lock is held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        ResourceEntry resourceEntry = getResourceEntry(name);
        return resourceEntry.getTransactionLockType(transaction.getTransNum());
    }

    /**
     * Returns a list of locks held on `name` in the order they were acquired.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns a list of locks held by `transaction` in the order they were acquired.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. Refer to the comments at the top of this file and LockContext.java.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Creates a lock context for the database. Refer to the comments at the top of this file and LockContext.java.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
