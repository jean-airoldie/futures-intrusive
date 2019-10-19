//! An asynchronously awaitable semaphore for synchronization between concurrently
//! executing futures.

use crate::{
    intrusive_singly_linked_list::{LinkedList, ListNode},
    waker::RWaker,
};
use abi_stable::{
    external_types::parking_lot::RMutex,
    std_types::{
        RArc,
        ROption::{self, *},
    },
    StableAbi,
};
use core::pin::Pin;
use futures_core::future::{FusedFuture, Future};
use futures_core::task::{Context, Poll};

/// Tracks how the future had interacted with the semaphore
#[derive(PartialEq, StableAbi, Debug, Copy, Clone)]
#[repr(u8)]
enum PollState {
    /// The task has never interacted with the semaphore.
    New,
    /// The task was added to the wait queue at the semaphore.
    Waiting,
    /// The task had previously waited on the semaphore, but was notified
    /// that the semaphore was released in the meantime and that the task
    /// thereby could retry.
    Notified,
    /// The task had been polled to completion.
    Done,
    /// The semaphore was closed because the task could be polled to
    /// completion.
    Cancelled,
}

/// Tracks the SemaphoreAcquireFuture waiting state.
#[derive(StableAbi)]
#[repr(C)]
struct WaitQueueEntry {
    /// The task handle of the waiting task
    task: ROption<RWaker>,
    /// Current polling state
    state: PollState,
    /// The amount of permits that should be obtained
    required_permits: usize,
}

impl WaitQueueEntry {
    /// Creates a new WaitQueueEntry
    fn new(required_permits: usize) -> WaitQueueEntry {
        WaitQueueEntry {
            task: RNone,
            state: PollState::New,
            required_permits,
        }
    }
}

/// Internal state of the `Semaphore`
#[derive(StableAbi)]
#[repr(C)]
struct SemaphoreState {
    is_fair: bool,
    is_closed: bool,
    permits: usize,
    waiters: LinkedList<WaitQueueEntry>,
}

impl SemaphoreState {
    fn new(is_fair: bool, permits: usize) -> Self {
        SemaphoreState {
            is_fair,
            is_closed: false,
            permits,
            waiters: LinkedList::new(),
        }
    }

    fn close(&mut self) {
        if self.is_closed {
            return;
        }
        self.is_closed = true;

        // Wakeup all waiters.
        let mut waiters = self.waiters.take();

        unsafe {
            for waiter in waiters.into_iter() {
                (*waiter).state = PollState::Cancelled;
                if let RSome(handle) = (*waiter).task.take() {
                    handle.wake();
                }
            }
        }
    }

    /// Wakes up the last waiter and removes it from the wait queue
    fn wakeup_waiters(&mut self) {
        // Wake as many tasks as the permits allow
        let mut available = self.permits;

        loop {
            let last_waiter = self.waiters.peek_last();
            if last_waiter.is_null() {
                return;
            }

            // Safety: We checked that the pointer is not null.
            // The ListNode also is guaranteed to be valid inside the Mutex, since
            // waiters need to remove themselves from the wait queue.
            unsafe {
                let last_waiter: &mut ListNode<WaitQueueEntry> = &mut (*last_waiter);

                // Check if enough permits are available for this waiter.
                // If not then a wakeup attempt won't be successful.
                if available < last_waiter.required_permits {
                    return;
                }
                available -= last_waiter.required_permits;

                // Notify the waiter that it can try to acquire the semaphore again.
                // The notification gets tracked inside the waiter.
                // If the waiter aborts it's wait (drops the future), another task
                // must be woken.
                if last_waiter.state != PollState::Notified {
                    last_waiter.state = PollState::Notified;

                    let task = &last_waiter.task;
                    if let RSome(ref handle) = task {
                        handle.wake_by_ref();
                    }
                }

                // In the case of a non-fair semaphore, the waiters are directly
                // removed from the semaphores wait queue when woken.
                // That avoids having to remove the wait element later.
                if !self.is_fair {
                    self.waiters.remove_last();
                } else {
                    // For a fair Semaphore we never wake more than 1 task.
                    // That one needs to acquire the Semaphore.
                    // TODO: We actually should be able to wake more, since
                    // it's guaranteed that both tasks could make progress.
                    // However the we currently can't peek iterate in reverse order.
                    return;
                }
            }
        }
    }

    fn permits(&self) -> usize {
        self.permits
    }

    /// Releases a certain amount of permits back to the semaphore
    fn release(&mut self, permits: usize) {
        if permits == 0 {
            return;
        }
        // TODO: Overflow check
        self.permits += permits;

        // Wakeup the last waiter
        self.wakeup_waiters();
    }

    /// Tries to acquire the given amount of permits synchronously.
    ///
    /// Returns true if the permits were obtained and false otherwise.
    fn try_acquire_sync(&mut self, required_permits: usize) -> bool {
        // Permits can only be obtained synchronously if
        // - semaphore is not closed
        // - enough permits available
        // - the Semaphore is either not fair, or there are no waiters
        // - required_permits == 0
        if !self.is_closed
            && (self.permits >= required_permits)
            && (!self.is_fair || self.waiters.is_empty() || required_permits == 0)
        {
            self.permits -= required_permits;
            true
        } else {
            false
        }
    }

    /// Tries to acquire the Semaphore from a WaitQueueEntry.
    /// If it isn't available, the WaitQueueEntry gets added to the wait
    /// queue at the Semaphore, and will be signalled once ready.
    /// This function is only safe as long as the `wait_node`s address is guaranteed
    /// to be stable until it gets removed from the queue.
    unsafe fn try_acquire(
        &mut self,
        wait_node: &mut ListNode<WaitQueueEntry>,
        cx: &mut Context<'_>,
    ) -> Poll<()> {
        match wait_node.state {
            PollState::New => {
                if self.is_closed {
                    wait_node.state = PollState::Cancelled;
                    Poll::Ready(())
                }
                // The fast path - enough permits are available
                else if self.try_acquire_sync(wait_node.required_permits) {
                    wait_node.state = PollState::Done;
                    Poll::Ready(())
                } else {
                    // Add the task to the wait queue
                    wait_node.task = RSome(RWaker::new(cx.waker().clone()));
                    wait_node.state = PollState::Waiting;
                    self.waiters.add_front(wait_node);
                    Poll::Pending
                }
            }
            PollState::Waiting => {
                if self.is_closed {
                    wait_node.state = PollState::Cancelled;
                    self.force_remove_waiter(wait_node);
                    Poll::Ready(())
                }
                // The SemaphoreAcquireFuture is already in the queue.
                else if self.is_fair {
                    // The task needs to wait until it gets notified in order to
                    // maintain the ordering.
                    Poll::Pending
                }
                // For throughput improvement purposes, check immediately
                // if enough permits are available
                else if self.permits >= wait_node.required_permits {
                    self.permits -= wait_node.required_permits;
                    wait_node.state = PollState::Done;
                    // Since this waiter has been registered before, it must
                    // get removed from the waiter list.
                    self.force_remove_waiter(wait_node);
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            PollState::Notified => {
                if self.is_closed {
                    wait_node.state = PollState::Cancelled;
                    self.force_remove_waiter(wait_node);
                    Poll::Ready(())
                }
                // We had been woken by the semaphore, since the semaphore is available again.
                // The semaphore thereby removed us from the waiters list.
                // Just try to lock again. If the semaphore isn't available,
                // we need to add it to the wait queue again.
                else if self.permits >= wait_node.required_permits {
                    if self.is_fair {
                        // In a fair Semaphore, the WaitQueueEntry is kept in the
                        // linked list and must be removed here
                        self.force_remove_waiter(wait_node);
                    }
                    self.permits -= wait_node.required_permits;
                    if self.is_fair {
                        // There might be another task which is ready to run,
                        // but couldn't, since it was blocked behind the fair waiter.
                        self.wakeup_waiters();
                    }
                    wait_node.state = PollState::Done;
                    Poll::Ready(())
                } else {
                    // A fair semaphore should never end up in that branch, since
                    // it's only notified when it's permits are guaranteed to
                    // be available. assert! in order to find logic bugs
                    assert!(
                        !self.is_fair,
                        "Fair semaphores should always be ready when notified"
                    );
                    // Add to queue
                    wait_node.task = RSome(RWaker::new(cx.waker().clone()));
                    wait_node.state = PollState::Waiting;
                    self.waiters.add_front(wait_node);
                    Poll::Pending
                }
            }
            PollState::Done => {
                // The future had been polled to completion before
                panic!("polled Mutex after completion");
            }
            PollState::Cancelled => {
                // Waked up by the semaphore being closed.
                Poll::Ready(())
            }
        }
    }

    /// Tries to remove a waiter from the wait queue, and panics if the
    /// waiter is no longer valid.
    unsafe fn force_remove_waiter(&mut self, wait_node: *mut ListNode<WaitQueueEntry>) {
        if !self.waiters.remove(wait_node) {
            // Panic if the address isn't found. This can only happen if the contract was
            // violated, e.g. the WaitQueueEntry got moved after the initial poll.
            panic!("Future could not be removed from wait queue");
        }
    }

    /// Removes the waiter from the list.
    /// This function is only safe as long as the reference that is passed here
    /// equals the reference/address under which the waiter was added.
    /// The waiter must not have been moved in between.
    fn remove_waiter(&mut self, wait_node: &mut ListNode<WaitQueueEntry>) {
        // SemaphoreAcquireFuture only needs to get removed if it had been added to
        // the wait queue of the Semaphore. This has happened in the PollState::Waiting case.
        // If the current waiter was notified, another waiter must get notified now.
        match wait_node.state {
            PollState::Notified => {
                if self.is_fair {
                    // In a fair Mutex, the WaitQueueEntry is kept in the
                    // linked list and must be removed here
                    unsafe { self.force_remove_waiter(wait_node) };
                }
                wait_node.state = PollState::Done;
                // Wakeup more waiters
                self.wakeup_waiters();
            }
            PollState::Waiting => {
                // Remove the WaitQueueEntry from the linked list
                unsafe { self.force_remove_waiter(wait_node) };
                wait_node.state = PollState::Done;
            }
            PollState::New | PollState::Done | PollState::Cancelled => {}
        }
    }
}

/// An RAII guard returned by the `acquire` and `try_acquire` methods.
///
/// When this structure is dropped (falls out of scope),
/// the amount of permits that was used in the `acquire()` call will be released
/// back to the Semaphore.
#[derive(StableAbi)]
#[repr(C)]
pub struct Releaser {
    /// The Semaphore which is associated with this Releaser
    semaphore: Semaphore,
    /// The amount of permits to release
    permits: usize,
}

impl core::fmt::Debug for Releaser {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("SemaphoreReleaser").finish()
    }
}

impl Releaser {
    /// Prevents the SemaphoreReleaser from automatically releasing the permits
    /// when it gets dropped.
    /// This is helpful if the permits must be acquired for a longer lifetime
    /// than the one of the SemaphoreReleaser.
    /// If this method is used it is important to release the acquired permits
    /// manually back to the Semaphore.
    pub fn disarm(&mut self) -> usize {
        let permits = self.permits;
        self.permits = 0;
        permits
    }
}

impl Drop for Releaser {
    fn drop(&mut self) {
        // Release the requested amount of permits to the semaphore
        if self.permits != 0 {
            self.semaphore.state.lock().release(self.permits);
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct AcquireError(());

/// A future which resolves when the target semaphore has been successfully acquired.
#[must_use = "futures do nothing unless polled"]
pub struct SemaphoreAcquire {
    /// The Semaphore which should get acquired trough this Future
    semaphore: Option<Semaphore>,
    /// Node for waiting at the semaphore
    wait_node: ListNode<WaitQueueEntry>,
    /// Whether the obtained permits should automatically be released back
    /// to the semaphore.
    auto_release: bool,
}

// Safety: Futures can be sent between threads as long as the underlying
// semaphore is thread-safe (Sync), which allows to poll/register/unregister from
// a different thread.
unsafe impl Send for SemaphoreAcquire {}

impl core::fmt::Debug for SemaphoreAcquire {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("SemaphoreAcquire").finish()
    }
}

impl Future for SemaphoreAcquire {
    type Output = Result<Releaser, AcquireError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Safety: The next operations are safe, because Pin promises us that
        // the address of the wait queue entry inside SemaphoreAcquireFuture is stable,
        // and we don't move any fields inside the future until it gets dropped.
        let mut_self: &mut SemaphoreAcquire = unsafe { Pin::get_unchecked_mut(self) };

        let semaphore = mut_self
            .semaphore
            .as_mut()
            .expect("polled SemaphoreAcquire after completion");

        let poll_res = {
            let mut semaphore_state = semaphore.state.lock();

            unsafe { semaphore_state.try_acquire(&mut mut_self.wait_node, cx) }
        };

        match poll_res {
            Poll::Pending => Poll::Pending,
            Poll::Ready(()) => {
                // The semaphore was closed before we could acquire it.
                if let PollState::Cancelled = mut_self.wait_node.state {
                    return Poll::Ready(Err(AcquireError(())));
                }
                // The semaphore was acquired.
                let to_release = match mut_self.auto_release {
                    true => mut_self.wait_node.required_permits,
                    false => 0,
                };
                Poll::Ready(Ok(Releaser {
                    semaphore: mut_self.semaphore.take().unwrap(),
                    permits: to_release,
                }))
            }
        }
    }
}

impl FusedFuture for SemaphoreAcquire {
    fn is_terminated(&self) -> bool {
        self.semaphore.is_none() || self.semaphore.as_ref().unwrap().is_closed()
    }
}

impl Drop for SemaphoreAcquire {
    fn drop(&mut self) {
        // The waiter was already unregistered, no need to do anything.
        if let PollState::Cancelled = self.wait_node.state {
            return;
        }
        // If this SemaphoreAcquireFuture has been polled and it was added to the
        // wait queue at the semaphore, it must be removed before dropping.
        // Otherwise the semaphore would access invalid memory.
        if let Some(semaphore) = self.semaphore.take() {
            let mut semaphore_state = semaphore.state.lock();
            // Analysis: Does the number of permits play a role here?
            // The future was notified because there was a certain amount of permits
            // available.
            // Removing the waiter will wake up as many tasks as there are permits
            // available inside the Semaphore now. If this is bigger than the
            // amount of permits required for this task, then additional new
            // tasks might get woken. However that isn't bad, since
            // those tasks should get into the wait state anyway.
            semaphore_state.remove_waiter(&mut self.wait_node);
        }
    }
}

/// A futures-aware semaphore.
#[derive(StableAbi)]
#[repr(C)]
pub struct Semaphore {
    state: RArc<RMutex<SemaphoreState>>,
}

// It is safe to send semaphores between threads, as long as they are not used and
// thereby borrowed
unsafe impl Send for Semaphore {}
// The Semaphore is thread-safe as long as the utilized Mutex is thread-safe
unsafe impl Sync for Semaphore {}

impl core::fmt::Debug for Semaphore {
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("Semaphore")
            .field("permits", &self.permits())
            .finish()
    }
}

impl Clone for Semaphore {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl Semaphore {
    /// Creates a new futures-aware semaphore.
    ///
    /// `is_fair` defines whether the `Semaphore` should behave be fair regarding the
    /// order of waiters. A fair `Semaphore` will only allow the oldest waiter on
    /// a `Semaphore` to retry acquiring it once it's available again.
    /// Other waiters must wait until either this acquire attempt completes, and
    /// the `Semaphore` has enough permits after that, or until the
    /// [`SemaphoreAcquire`] which tried to acquire the `Semaphore` is dropped.
    ///
    /// If the `Semaphore` isn't fair, waiters that wait for a high amount of
    /// permits might never succeed since the permits might be stolen in between
    /// by other waiters. Therefore use-cases which make use of very different
    /// amount of permits per acquire should use fair semaphores.
    /// For use-cases where each `acquire()` tries to acquire the same amount of
    /// permits an unfair `Semaphore` might provide throughput advantages.
    ///
    /// `permits` is the amount of permits that a semaphore should hold when
    /// created.
    pub fn new(is_fair: bool, permits: usize) -> Semaphore {
        Semaphore {
            state: RArc::new(RMutex::new(SemaphoreState::new(is_fair, permits))),
        }
    }

    /// Acquire a certain amount of permits on a semaphore asynchronously.
    ///
    /// This method returns a future that will resolve once the given amount of
    /// permits have been acquired.
    /// The Future will resolve to a [`SemaphoreReleaser`], which will
    /// release all acquired permits automatically when dropped.
    pub fn acquire(&self, nr_permits: usize) -> SemaphoreAcquire {
        SemaphoreAcquire {
            semaphore: Some(self.clone()),
            wait_node: ListNode::new(WaitQueueEntry::new(nr_permits)),
            auto_release: true,
        }
    }

    /// Tries to acquire a certain amount of permits on a semaphore.
    ///
    /// If acquiring the permits is successful, a [`SemaphoreReleaser`]
    /// will be returned, which will release all acquired permits automatically
    /// when dropped.
    ///
    /// Otherwise `None` will be returned.
    pub fn try_acquire(&self, nr_permits: usize) -> Option<Releaser> {
        if self.state.lock().try_acquire_sync(nr_permits) {
            Some(Releaser {
                semaphore: self.clone(),
                permits: nr_permits,
            })
        } else {
            None
        }
    }

    /// Releases the given amount of permits back to the semaphore.
    ///
    /// This method should in most cases not be used, since the
    /// [`SemaphoreReleaser`] which is obtained when acquiring a Semaphore
    /// will automatically release the obtained permits again.
    ///
    /// Therefore this method should only be used if the automatic release was
    /// disabled by calling [`SemaphoreReleaser::disarm`],
    /// or when the amount of permits in the Semaphore
    /// should increase from the initial amount.
    pub fn release(&self, nr_permits: usize) {
        self.state.lock().release(nr_permits)
    }

    /// Returns the amount of permits that are available on the semaphore
    pub fn permits(&self) -> usize {
        self.state.lock().permits()
    }

    /// Prevents the `Semaphore` from being acquired from not on, waking up all
    /// waiters.
    ///
    /// Attempting to `acquire` the `Semaphore` will return an `AcquireError`
    /// from now on.
    pub fn close(&self) {
        self.state.lock().close()
    }

    /// Returns whether the semaphore was closed.
    pub fn is_closed(&self) -> bool {
        self.state.lock().is_closed
    }
}

#[cfg(test)]
mod tests {
    use super::Semaphore;
    use futures::future::{FusedFuture, Future, FutureExt};
    use futures::task::{Context, Poll};
    use futures_test::task::{new_count_waker, panic_waker};
    use pin_utils::pin_mut;

    #[test]
    fn uncontended_acquire() {
        for is_fair in &[true, false] {
            let waker = &panic_waker();
            let cx = &mut Context::from_waker(&waker);
            let sem = Semaphore::new(*is_fair, 2);
            assert_eq!(2, sem.permits());

            {
                let sem_fut = sem.acquire(1);
                pin_mut!(sem_fut);
                match sem_fut.as_mut().poll(cx) {
                    Poll::Pending => panic!("Expect semaphore to get acquired"),
                    Poll::Ready(_guard) => {
                        assert_eq!(1, sem.permits());
                    }
                };
                assert!(sem_fut.as_mut().is_terminated());
                assert_eq!(2, sem.permits());
            }
            assert_eq!(2, sem.permits());

            {
                let sem_fut = sem.acquire(2);
                pin_mut!(sem_fut);
                match sem_fut.as_mut().poll(cx) {
                    Poll::Pending => panic!("Expect semaphore to get acquired"),
                    Poll::Ready(_guard) => {
                        assert_eq!(0, sem.permits());
                    }
                };
                assert!(sem_fut.as_mut().is_terminated());
            }

            assert_eq!(2, sem.permits());
        }
    }

    #[test]
    fn closing_prevents_acquire() {
        let mut sem = Semaphore::new(true, 2);
        sem.close();

        let waker = &panic_waker();
        let cx = &mut Context::from_waker(&waker);

        let sem_fut = sem.acquire(2);
        pin_mut!(sem_fut);
        match sem_fut.as_mut().poll(cx) {
            Poll::Pending => panic!("Expect semaphore to get acquired"),
            Poll::Ready(res) => {
                let err = res.unwrap_err();
            }
        };
        assert!(sem_fut.as_mut().is_terminated());
    }

    #[test]
    fn closing_wakes_waiters() {
        let mut sem = Semaphore::new(true, 1);

        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let sem_fut = sem.acquire(2);
        pin_mut!(sem_fut);
        match sem_fut.as_mut().poll(cx) {
            Poll::Pending => (),
            Poll::Ready(_) => {
                panic!("Expect semaphore to not get acquired");
            }
        };

        assert_eq!(count, 0);
        sem.close();
        assert_eq!(count, 1);

        let sem_fut = sem.acquire(2);
        pin_mut!(sem_fut);
        match sem_fut.as_mut().poll(cx) {
            Poll::Pending => panic!("Expect semaphore to get acquired"),
            Poll::Ready(res) => {
                let err = res.unwrap_err();
            }
        };
        assert!(sem_fut.as_mut().is_terminated());
    }

    #[test]
    fn manual_release_via_disarm() {
        for is_fair in &[true, false] {
            let waker = &panic_waker();
            let cx = &mut Context::from_waker(&waker);
            let sem = Semaphore::new(*is_fair, 2);
            assert_eq!(2, sem.permits());

            {
                let sem_fut = sem.acquire(1);
                pin_mut!(sem_fut);
                match sem_fut.as_mut().poll(cx) {
                    Poll::Pending => panic!("Expect semaphore to get acquired"),
                    Poll::Ready(mut res) => {
                        assert_eq!(1, sem.permits());
                        res.unwrap().disarm();
                    }
                };
                assert!(sem_fut.as_mut().is_terminated());
                assert_eq!(1, sem.permits());
            }

            assert_eq!(1, sem.permits());

            {
                let sem_fut = sem.acquire(1);
                pin_mut!(sem_fut);
                match sem_fut.as_mut().poll(cx) {
                    Poll::Pending => panic!("Expect semaphore to get acquired"),
                    Poll::Ready(mut res) => {
                        assert_eq!(0, sem.permits());
                        res.unwrap().disarm();
                    }
                };
                assert!(sem_fut.as_mut().is_terminated());
            }

            assert_eq!(0, sem.permits());

            sem.release(2);
            assert_eq!(2, sem.permits());
        }
    }

    #[test]
    #[should_panic]
    fn poll_after_completion_should_panic() {
        for is_fair in &[true, false] {
            let waker = &panic_waker();
            let cx = &mut Context::from_waker(&waker);
            let sem = Semaphore::new(*is_fair, 2);

            let sem_fut = sem.acquire(2);
            pin_mut!(sem_fut);
            match sem_fut.as_mut().poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired"),
                Poll::Ready(guard) => guard,
            };
            assert!(sem_fut.as_mut().is_terminated());

            let _ = sem_fut.poll(cx);
        }
    }

    #[test]
    fn contended_acquire() {
        for is_fair in &[false, true] {
            let (waker, count) = new_count_waker();
            let cx = &mut Context::from_waker(&waker);
            let sem = Semaphore::new(*is_fair, 3);

            let sem_fut1 = sem.acquire(3);
            pin_mut!(sem_fut1);

            // Acquire the semaphore
            let guard1 = match sem_fut1.poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired 1"),
                Poll::Ready(guard) => guard,
            };

            // The next acquire attempts must fail
            let sem_fut2 = sem.acquire(1);
            pin_mut!(sem_fut2);
            assert!(sem_fut2.as_mut().poll(cx).is_pending());
            assert!(!sem_fut2.as_mut().is_terminated());
            let sem_fut3 = sem.acquire(2);
            pin_mut!(sem_fut3);
            assert!(sem_fut3.as_mut().poll(cx).is_pending());
            assert!(!sem_fut3.as_mut().is_terminated());
            let sem_fut4 = sem.acquire(2);
            pin_mut!(sem_fut4);
            assert!(sem_fut4.as_mut().poll(cx).is_pending());
            assert!(!sem_fut4.as_mut().is_terminated());
            assert_eq!(count, 0);

            // Release - semaphore should be available again and allow
            // fut2 and fut3 to complete
            assert_eq!(0, sem.permits());
            drop(guard1);
            assert_eq!(3, sem.permits());
            // At least one task should be awoken.
            if *is_fair {
                assert_eq!(count, 1);
            } else {
                assert_eq!(count, 2);
            }

            let guard2 = match sem_fut2.as_mut().poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired 2"),
                Poll::Ready(guard) => guard,
            };
            assert!(sem_fut2.as_mut().is_terminated());
            assert_eq!(2, sem.permits());
            // In the fair case, the next task should be woken up here
            assert_eq!(count, 2);

            let guard3 = match sem_fut3.as_mut().poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired 3"),
                Poll::Ready(guard) => guard,
            };
            assert!(sem_fut3.as_mut().is_terminated());
            assert_eq!(0, sem.permits());

            assert!(sem_fut4.as_mut().poll(cx).is_pending());
            assert!(!sem_fut4.as_mut().is_terminated());

            // Release - some permits should be available again
            drop(guard2);
            assert_eq!(1, sem.permits());
            assert_eq!(count, 2);

            assert!(sem_fut4.as_mut().poll(cx).is_pending());
            assert!(!sem_fut4.as_mut().is_terminated());

            // After releasing the permits from fut3, there should be
            // enough permits for fut4 getting woken.
            drop(guard3);
            assert_eq!(3, sem.permits());
            assert_eq!(count, 3);

            let guard4 = match sem_fut4.as_mut().poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired 4"),
                Poll::Ready(guard) => guard,
            };
            assert!(sem_fut4.as_mut().is_terminated());

            drop(guard4);
            assert_eq!(3, sem.permits());
            assert_eq!(count, 3);
        }
    }

    #[test]
    fn acquire_synchronously() {
        for is_fair in &[true] {
            let (waker, count) = new_count_waker();
            let cx = &mut Context::from_waker(&waker);
            let sem = Semaphore::new(*is_fair, 3);

            let sem_fut1 = sem.acquire(3);
            pin_mut!(sem_fut1);

            // Acquire the semaphore
            let guard1 = match sem_fut1.poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired 1"),
                Poll::Ready(guard) => guard,
            };

            // Some failing acquire attempts
            assert!(sem.try_acquire(1).is_none());

            // Add an async waiter
            let mut sem_fut2 = Box::pin(sem.acquire(1));
            assert!(sem_fut2.as_mut().poll(cx).is_pending());
            assert_eq!(count, 0);

            // Release - semaphore should be available again
            drop(guard1);
            assert_eq!(3, sem.permits());

            // In the fair case we shouldn't be able to obtain the
            // semaphore asynchronously. In the unfair case it should
            // be possible.
            if *is_fair {
                assert!(sem.try_acquire(1).is_none());

                // Cancel async acquire attempt
                drop(sem_fut2);
                // Now the semaphore should be acquireable
            }

            let guard = sem.try_acquire(1).unwrap();
            assert_eq!(2, sem.permits());
            let mut guard2 = sem.try_acquire(2).unwrap();
            assert_eq!(0, sem.permits());
            guard2.disarm();
            sem.release(2);
            drop(guard);
        }
    }

    #[test]
    fn acquire_0_permits_without_other_waiters() {
        for is_fair in &[false, true] {
            let (waker, _count) = new_count_waker();
            let cx = &mut Context::from_waker(&waker);
            let sem = Semaphore::new(*is_fair, 3);

            // Acquire the semaphore
            let guard1 = sem.try_acquire(3).unwrap();
            assert_eq!(0, sem.permits());

            let sem_fut2 = sem.acquire(0);
            pin_mut!(sem_fut2);
            let guard2 = match sem_fut2.as_mut().poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired 2"),
                Poll::Ready(guard) => guard,
            };

            drop(guard2);
            assert_eq!(0, sem.permits());
            drop(guard1);
            assert_eq!(3, sem.permits());
        }
    }

    #[test]
    fn acquire_0_permits_with_other_waiters() {
        for is_fair in &[false, true] {
            let (waker, _count) = new_count_waker();
            let cx = &mut Context::from_waker(&waker);
            let sem = Semaphore::new(*is_fair, 3);

            // Acquire the semaphore
            let guard1 = sem.try_acquire(3).unwrap();

            assert_eq!(0, sem.permits());

            let sem_fut2 = sem.acquire(1);
            pin_mut!(sem_fut2);
            assert!(sem_fut2.as_mut().poll(cx).is_pending());

            let sem_fut3 = sem.acquire(0);
            pin_mut!(sem_fut3);
            let guard3 = match sem_fut3.as_mut().poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired 3"),
                Poll::Ready(guard) => guard,
            };

            drop(guard3);
            assert_eq!(0, sem.permits());
            drop(guard1);
            assert_eq!(3, sem.permits());

            let guard2 = match sem_fut2.as_mut().poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired 2"),
                Poll::Ready(guard) => guard,
            };
            assert_eq!(2, sem.permits());
            drop(guard2);
        }
    }

    #[test]
    fn cancel_wait_for_semaphore() {
        for is_fair in &[true, false] {
            let (waker, count) = new_count_waker();
            let cx = &mut Context::from_waker(&waker);
            let sem = Semaphore::new(*is_fair, 5);

            // Acquire the semaphore
            let guard1 = sem.try_acquire(5).unwrap();

            // The second and third lock attempt must fail
            let mut sem_fut2 = Box::pin(sem.acquire(1));
            let mut sem_fut3 = Box::pin(sem.acquire(1));

            assert!(sem_fut2.as_mut().poll(cx).is_pending());
            assert!(sem_fut3.as_mut().poll(cx).is_pending());

            // Before the semaphore gets available, cancel one acquire attempt
            drop(sem_fut2);

            // Unlock - semaphore should be available again.
            // fut2 should have been notified
            drop(guard1);
            assert_eq!(count, 1);

            // Unlock - semaphore should be available again
            match sem_fut3.as_mut().poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired"),
                Poll::Ready(guard) => guard,
            };
        }
    }

    #[test]
    fn unlock_next_when_notification_is_not_used() {
        for is_fair in &[true, false] {
            let (waker, count) = new_count_waker();
            let cx = &mut Context::from_waker(&waker);
            let sem = Semaphore::new(*is_fair, 2);

            let guard1 = sem.try_acquire(2).unwrap();

            // The second and third acquire attempt must fail
            let mut sem_fut2 = Box::pin(sem.acquire(1));
            let mut sem_fut3 = Box::pin(sem.acquire(1));

            assert!(sem_fut2.as_mut().poll(cx).is_pending());
            assert!(!sem_fut2.as_mut().is_terminated());
            assert!(sem_fut3.as_mut().poll(cx).is_pending());
            assert!(!sem_fut3.as_mut().is_terminated());
            assert_eq!(count, 0);

            // Release - semaphore should be available again. fut2 should have been notified
            drop(guard1);
            if *is_fair {
                assert_eq!(count, 1);
            } else {
                assert_eq!(count, 2);
            }

            // We don't use the notification. Expect the next waiting task to be woken up
            drop(sem_fut2);
            assert_eq!(count, 2);

            match sem_fut3.as_mut().poll(cx) {
                Poll::Pending => panic!("Expect semaphore to get acquired"),
                Poll::Ready(guard) => guard,
            };
        }
    }

    #[test]
    fn new_waiters_on_unfair_semaphore_can_acquire_future_while_one_task_is_notified() {
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);
        let sem = Semaphore::new(false, 3);

        // Acquire the semaphore
        let guard1 = sem.try_acquire(3).unwrap();

        // The second and third acquire attempt must fail
        let mut sem_fut2 = Box::pin(sem.acquire(3));
        let mut sem_fut3 = Box::pin(sem.acquire(3));

        assert!(sem_fut2.as_mut().poll(cx).is_pending());

        // Release - Semaphore should be available again. fut2 should have been notified
        drop(guard1);
        assert_eq!(count, 1);

        // Acquire fut3 in between. This should succeed
        let guard3 = match sem_fut3.as_mut().poll(cx) {
            Poll::Pending => panic!("Expect semaphore to get acquired"),
            Poll::Ready(guard) => guard,
        };
        // Now fut2 can't use it's notification and is still pending
        assert!(sem_fut2.as_mut().poll(cx).is_pending());

        // When we drop fut3, the semaphore should signal that it's available for fut2,
        // which needs to have re-registered
        drop(guard3);
        assert_eq!(count, 2);
        match sem_fut2.as_mut().poll(cx) {
            Poll::Pending => panic!("Expect semaphore to get acquired"),
            Poll::Ready(_guard) => {}
        };
    }

    #[test]
    fn waiters_on_unfair_semaphore_can_acquire_future_through_repolling_if_one_task_is_notified() {
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);
        let sem = Semaphore::new(false, 3);

        // Acquire the semaphore
        let guard1 = sem.try_acquire(3).unwrap();

        // The second and third acquire attempt must fail
        let mut sem_fut2 = Box::pin(sem.acquire(3));
        let mut sem_fut3 = Box::pin(sem.acquire(3));
        // Start polling both futures, which means both are waiters
        assert!(sem_fut2.as_mut().poll(cx).is_pending());
        assert!(sem_fut3.as_mut().poll(cx).is_pending());

        // Release - semaphore should be available again. fut2 should have been notified
        drop(guard1);
        assert_eq!(count, 1);

        // Acquire fut3 in between. This should succeed
        let guard3 = match sem_fut3.as_mut().poll(cx) {
            Poll::Pending => panic!("Expect semaphore to get acquired"),
            Poll::Ready(guard) => guard,
        };
        // Now fut2 can't use it's notification and is still pending
        assert!(sem_fut2.as_mut().poll(cx).is_pending());

        // When we drop fut3, the mutex should signal that it's available for fut2,
        // which needs to have re-registered
        drop(guard3);
        assert_eq!(count, 2);
        match sem_fut2.as_mut().poll(cx) {
            Poll::Pending => panic!("Expect semaphore to get acquired"),
            Poll::Ready(_guard) => {}
        };
    }

    #[test]
    fn new_waiters_on_fair_semaphore_cant_acquire_future_while_one_task_is_notified() {
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);
        let sem = Semaphore::new(true, 3);

        // Acquire the semaphore
        let guard1 = sem.try_acquire(3).unwrap();

        // The second and third acquire attempt must fail
        let mut sem_fut2 = Box::pin(sem.acquire(3));
        let mut sem_fut3 = Box::pin(sem.acquire(3));

        assert!(sem_fut2.as_mut().poll(cx).is_pending());

        // Release - semaphore should be available again. fut2 should have been notified
        drop(guard1);
        assert_eq!(count, 1);

        // Try to acquire fut3 in between. This should fail
        assert!(sem_fut3.as_mut().poll(cx).is_pending());

        // fut2 should be be able to get acquired
        match sem_fut2.as_mut().poll(cx) {
            Poll::Pending => panic!("Expect semaphore to get acquired"),
            Poll::Ready(_guard) => {}
        };

        // Now fut3 should have been signaled and should be able to get acquired
        assert_eq!(count, 2);
        match sem_fut3.as_mut().poll(cx) {
            Poll::Pending => panic!("Expect semaphore to get acquired"),
            Poll::Ready(_guard) => {}
        };
    }

    #[test]
    fn waiters_on_fair_semaphore_cant_acquire_future_through_repolling_if_one_task_is_notified() {
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);
        let sem = Semaphore::new(true, 3);

        // Acquire the semaphore
        let guard1 = sem.try_acquire(3).unwrap();

        // The second and third acquire attempt must fail
        let mut sem_fut2 = Box::pin(sem.acquire(3));
        let mut sem_fut3 = Box::pin(sem.acquire(3));

        assert!(sem_fut2.as_mut().poll(cx).is_pending());
        assert!(sem_fut3.as_mut().poll(cx).is_pending());

        // Release - semaphore should be available again. fut2 should have been notified
        drop(guard1);
        assert_eq!(count, 1);

        // Acquire fut3 in between. This should fail, since fut2 should get the permits first
        assert!(sem_fut3.as_mut().poll(cx).is_pending());

        // fut2 should be acquired
        match sem_fut2.as_mut().poll(cx) {
            Poll::Pending => panic!("Expect semaphore to get acquired"),
            Poll::Ready(_guard) => {}
        };

        // Now fut3 should be able to get acquired
        assert_eq!(count, 2);

        match sem_fut3.as_mut().poll(cx) {
            Poll::Pending => panic!("Expect semaphore to get acquired"),
            Poll::Ready(_guard) => {}
        };
    }

    fn is_send<T: Send>(_: &T) {}

    fn is_send_value<T: Send>(_: T) {}

    fn is_sync<T: Sync>(_: &T) {}

    #[test]
    fn semaphore_futures_are_send() {
        let sem = Semaphore::new(true, 3);
        is_sync(&sem);
        {
            let wait_fut = sem.acquire(3);
            is_send(&wait_fut);
            pin_mut!(wait_fut);
            is_send(&wait_fut);

            let waker = &panic_waker();
            let cx = &mut Context::from_waker(&waker);
            pin_mut!(wait_fut);
            let res = wait_fut.poll_unpin(cx);
            let releaser = match res {
                Poll::Ready(v) => v,
                Poll::Pending => panic!("Expected to be ready"),
            };
            is_send(&releaser);
            is_send_value(releaser);
        }
        is_send_value(sem);
    }
}
