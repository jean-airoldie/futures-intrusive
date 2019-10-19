//! An asynchronously awaitable multi producer multi consumer channel

use super::SendError;
use crate::{
    intrusive_singly_linked_list::{LinkedList, ListNode},
    sync::{Releaser, Semaphore},
    waker::RWaker,
};
use abi_stable::{
    external_types::parking_lot::RMutex,
    std_types::{
        RArc,
        ROption::{self, *},
        RVec,
    },
    StableAbi,
};
use core::marker::PhantomData;
use futures_core::{
    task::{Context, Poll},
    Future, Stream,
};
use futures_util::{future, pin_mut, FutureExt};
use lock_api::{Mutex, RawMutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::VecDeque, pin::Pin, sync::Arc};

use super::{
    ChanRecv, ChanRecvAccess, ChanSend, RecvPollState, RecvWaitQueueEntry, SendPollState,
    SendWaitQueueEntry,
};

fn wake_recv_waiters(mut waiters: LinkedList<RecvWaitQueueEntry>) {
    unsafe {
        // Reverse the waiter list, so that the oldest waker (which is
        // at the end of the list), gets woken first and has the best
        // chance to grab the channel value.
        waiters.reverse();

        for waiter in waiters.into_iter() {
            if let RSome(handle) = (*waiter).task.take() {
                handle.wake();
            }
            // The only kind of waiter that could have been stored here are
            // registered waiters (with a value), since others are removed
            // whenever their value had been copied into the channel.
            (*waiter).state = RecvPollState::Unregistered;
        }
    }
}

fn wake_send_waiters<T: StableAbi>(mut waiters: LinkedList<SendWaitQueueEntry<T>>) {
    unsafe {
        // Reverse the waiter list, so that the oldest waker (which is
        // at the end of the list), gets woken first and has the best
        // chance to grab the channel value.
        waiters.reverse();

        for waiter in waiters.into_iter() {
            if let RSome(handle) = (*waiter).task.take() {
                handle.wake();
            }
            (*waiter).state = SendPollState::Unregistered;
        }
    }
}

/// Wakes up the last waiter and removes it from the wait queue
fn wakeup_last_recv_waiter(waiters: &mut LinkedList<RecvWaitQueueEntry>) {
    let last_waiter = waiters.remove_last();

    if !last_waiter.is_null() {
        unsafe {
            (*last_waiter).state = RecvPollState::Notified;

            if let RSome(handle) = (*last_waiter).task.take() {
                handle.wake();
            }
        }
    }
}

/// Internal state of the channel
#[derive(StableAbi)]
#[repr(C)]
struct MpmcState<T>
where
    T: StableAbi + Send,
{
    /// Whether the channel had been closed
    is_closed: bool,
    /// The value which is stored inside the channel
    /// FIXME use a ring buffer for decent performance.
    buffer: RVec<ReleaserGuard<T>>,
    /// Futures which are blocked on recv
    recv_waiters: LinkedList<RecvWaitQueueEntry>,
}

impl<T> MpmcState<T>
where
    T: StableAbi + Send,
{
    fn new() -> Self {
        MpmcState {
            is_closed: false,
            buffer: RVec::new(),
            recv_waiters: LinkedList::new(),
        }
    }

    fn with_capacity(capacity: usize) -> Self {
        MpmcState {
            is_closed: false,
            buffer: RVec::with_capacity(capacity),
            recv_waiters: LinkedList::new(),
        }
    }

    fn close(&mut self) {
        if self.is_closed {
            return;
        }
        self.is_closed = true;

        // Wakeup all send and recv waiters, since they are now guaranteed
        // to make progress.
        let recv_waiters = self.recv_waiters.take();
        wake_recv_waiters(recv_waiters);
    }

    fn send(&mut self, guard: ReleaserGuard<T>) -> Result<(), SendError<T>> {
        if !self.is_closed {
            self.buffer.insert(0, guard);

            // Wakeup the oldest recv waiter
            wakeup_last_recv_waiter(&mut self.recv_waiters);
            Ok(())
        } else {
            Err(SendError(guard.value))
        }
    }

    /// Tries to read the value from the channel.
    /// If the value isn't available yet, the ChanRecvFuture gets added to the
    /// wait queue at the channel, and will be signalled once ready.
    /// This function is only safe as long as the `wait_node`s address is guaranteed
    /// to be stable until it gets removed from the queue.
    unsafe fn try_recv(
        &mut self,
        wait_node: &mut ListNode<RecvWaitQueueEntry>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<T>> {
        match wait_node.state {
            RecvPollState::Unregistered | RecvPollState::Notified => {
                wait_node.state = RecvPollState::Unregistered;

                if !self.buffer.is_empty() {
                    // A value is available - grab it.
                    let val = self.buffer.pop().unwrap().value;

                    Poll::Ready(Some(val))
                } else if self.is_closed {
                    Poll::Ready(None)
                } else {
                    // Added the task to the wait queue
                    wait_node.task = RSome(RWaker::new(cx.waker().clone()));
                    wait_node.state = RecvPollState::Registered;
                    self.recv_waiters.add_front(wait_node);
                    Poll::Pending
                }
            }
            RecvPollState::Registered => {
                // Since the channel wakes up all waiters and moves their states
                // to unregistered there can't be any value in the channel in
                // this state.
                Poll::Pending
            }
        }
    }

    fn remove_recv_waiter(&mut self, wait_node: &mut ListNode<RecvWaitQueueEntry>) {
        // ChanRecvFuture only needs to get removed if it had been added to
        // the wait queue of the channel. This has happened in the RecvPollState::Registered case.
        match wait_node.state {
            RecvPollState::Registered => {
                if !unsafe { self.recv_waiters.remove(wait_node) } {
                    // Panic if the address isn't found. This can only happen if the contract was
                    // violated, e.g. the WaitQueueEntry got moved after the initial poll.
                    panic!("Future could not be removed from wait queue");
                }
                wait_node.state = RecvPollState::Unregistered;
            }
            RecvPollState::Notified => {
                // wakeup another recv waiter instead
                wakeup_last_recv_waiter(&mut self.recv_waiters);
                wait_node.state = RecvPollState::Unregistered;
            }
            RecvPollState::Unregistered => {}
        }
    }
}

#[derive(StableAbi)]
#[repr(C)]
struct ReleaserGuard<T> {
    value: T,
    permits: Releaser,
}

/// A channel which can be used to exchange values of type `T` between
/// concurrent tasks.
///
/// `A` represents the backing buffer for a Chan. E.g. a channel which
/// can buffer up to 4 u32 values can be created via:
///
/// ```
/// # use futures_intrusive::channel::LocalChan;
/// let channel: LocalChan<i32, [i32; 4]> = LocalChan::new();
/// ```
///
/// Tasks can recv values from the channel through the `recv` method.
/// The returned Future will get resolved when a value is sent into the channel.
/// Values can be sent into the channel through `send`.
/// The returned Future will get resolved when the value has been stored
/// inside the channel.
struct MpmcChan<T>
where
    T: StableAbi + Send,
{
    inner: RArc<RMutex<MpmcState<T>>>,
    sem: Semaphore,
}

impl<T> Clone for MpmcChan<T>
where
    T: StableAbi + Send,
{
    fn clone(&self) -> Self {
        MpmcChan {
            inner: self.inner.clone(),
            sem: self.sem.clone(),
        }
    }
}

impl<T> MpmcChan<T>
where
    T: StableAbi + Send,
{
    fn new(permits: usize) -> Self {
        MpmcChan {
            inner: RArc::new(RMutex::new(MpmcState::new())),
            sem: Semaphore::new(true, permits),
        }
    }

    fn with_capacity(permits: usize, capacity: usize) -> Self {
        MpmcChan {
            inner: RArc::new(RMutex::new(MpmcState::with_capacity(capacity))),
            sem: Semaphore::new(true, permits),
        }
    }

    async fn send(&self, value: T, permits: usize) -> Result<(), SendError<T>> {
        match self.sem.acquire(permits).await {
            Ok(permits) => {
                let guard = ReleaserGuard { value, permits };
                self.inner.lock().send(guard)
            }
            Err(_) => Err(SendError(value)),
        }
    }

    /// Returns a future that gets fulfilled when a value is written to the channel.
    /// If the channels gets closed, the future will resolve to `None`.
    fn recv(&self) -> ChanRecv<MpmcChan<T>> {
        ChanRecv {
            channel: Some(self.clone()),
            wait_node: ListNode::new(RecvWaitQueueEntry::new()),
        }
    }

    /// Closes the channel.
    /// All pending and future send attempts will fail.
    /// Recv attempts will continue to succeed as long as there are items
    /// stored inside the channel. Further attempts will fail.
    fn close(&self) {
        self.inner.lock().close();
        self.sem.close();
    }
}

// The channel can be sent to other threads as long as it's not borrowed and the
// value in it can be sent to other threads.
unsafe impl<T: StableAbi + Send> Send for MpmcChan<T> {}
// The channel is thread-safe as long as a thread-safe mutex is used
unsafe impl<T: StableAbi + Send> Sync for MpmcChan<T> {}

impl<T> core::fmt::Debug for MpmcChan<T>
where
    T: StableAbi + Send,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("Chan").finish()
    }
}

impl<T> ChanRecvAccess for MpmcChan<T>
where
    T: Send + StableAbi,
{
    type Item = T;

    unsafe fn try_recv(
        &self,
        wait_node: &mut ListNode<RecvWaitQueueEntry>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.inner.lock().try_recv(wait_node, cx)
    }

    fn remove_recv_waiter(&self, wait_node: &mut ListNode<RecvWaitQueueEntry>) {
        self.inner.lock().remove_recv_waiter(wait_node)
    }
}

/// Shared Chan State, which is referenced by MpmcTxs and MpmcRxs
struct ChanSharedState<T>
where
    T: Send + StableAbi,
{
    /// The amount of [`MpmcTx`] instances which reference this state.
    txs: AtomicUsize,
    /// The amount of [`MpmcRx`] instances which reference this state.
    rxs: AtomicUsize,
    /// The channel on which is acted.
    channel: MpmcChan<T>,
}

// Implement ChanAccess trait for SharedChanState, so that it can
// be used for dynamic dispatch in futures.
impl<T> ChanRecvAccess for ChanSharedState<T>
where
    T: Send + StableAbi,
{
    type Item = T;

    unsafe fn try_recv(
        &self,
        wait_node: &mut ListNode<RecvWaitQueueEntry>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.channel.try_recv(wait_node, cx)
    }

    fn remove_recv_waiter(&self, wait_node: &mut ListNode<RecvWaitQueueEntry>) {
        self.channel.remove_recv_waiter(wait_node)
    }
}

/// The sending side of a channel which can be used to exchange values
/// between concurrent tasks.
///
/// Values can be sent into the channel through `send`.
/// The returned Future will get resolved when the value has been stored inside the channel.
pub struct MpmcTx<T>
where
    T: Send + StableAbi,
{
    inner: Arc<ChanSharedState<T>>,
}

/// The receiving side of a channel which can be used to exchange values
/// between concurrent tasks.
///
/// Tasks can recv values from the channel through the `recv` method.
/// The returned Future will get resolved when a value is sent into the channel.
pub struct MpmcRx<T>
where
    T: Send + StableAbi,
{
    inner: Arc<ChanSharedState<T>>,
}

impl<T> core::fmt::Debug for MpmcTx<T>
where
    T: Send + StableAbi,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("MpmcTx").finish()
    }
}

impl<T> core::fmt::Debug for MpmcRx<T>
where
    T: Send + StableAbi,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("MpmcRx").finish()
    }
}

impl<T> Clone for MpmcTx<T>
where
    T: Send + StableAbi,
{
    fn clone(&self) -> Self {
        let old_size = self.inner.txs.fetch_add(1, Ordering::Relaxed);
        if old_size > (core::isize::MAX) as usize {
            panic!("Reached maximum refcount");
        }
        MpmcTx {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for MpmcTx<T>
where
    T: Send + StableAbi,
{
    fn drop(&mut self) {
        if self.inner.txs.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        std::sync::atomic::fence(Ordering::Acquire);
        // Close the channel, before last sender gets destroyed
        // TODO: We could potentially avoid this, if no recvr is left
        self.inner.channel.close();
    }
}

impl<T> Clone for MpmcRx<T>
where
    T: Send + StableAbi,
{
    fn clone(&self) -> Self {
        let old_size = self.inner.rxs.fetch_add(1, Ordering::Relaxed);
        if old_size > (core::isize::MAX) as usize {
            panic!("Reached maximum refcount");
        }
        MpmcRx {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for MpmcRx<T>
where
    T: Send + StableAbi,
{
    fn drop(&mut self) {
        if self.inner.rxs.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        std::sync::atomic::fence(Ordering::Acquire);
        // Close the channel, before last receiver gets destroyed
        // TODO: We could potentially avoid this, if no sender is left
        self.inner.channel.close();
    }
}

/// Creates a new Chan which can be used to exchange values of type `T` between
/// concurrent tasks. The ends of the Chan are represented through
/// the returned MpmcTx and MpmcRx.
/// Both the MpmcTx and MpmcRx can be cloned in order to let more tasks
/// interact with the Chan.
///
/// As soon es either all MpmcTxs or all MpmcRxs are closed, the Chan
/// itself will be closed.
///
/// The channel can buffer up to `capacity` items internally.
///
/// ```
/// # use futures_intrusive::channel::shared::channel;
/// let (sender, recvr) = channel::<i32>(4);
/// ```
pub fn mpmc_chan<T>(permits: usize) -> (MpmcTx<T>, MpmcRx<T>)
where
    T: Send + StableAbi,
{
    let inner = std::sync::Arc::new(ChanSharedState {
        channel: MpmcChan::new(permits),
        txs: AtomicUsize::new(1),
        rxs: AtomicUsize::new(1),
    });

    let sender = MpmcTx {
        inner: inner.clone(),
    };
    let recvr = MpmcRx { inner };

    (sender, recvr)
}

impl<T> MpmcTx<T>
where
    T: Send + StableAbi,
{
    pub async fn send(&self, value: T, permits: usize) -> Result<(), SendError<T>> {
        self.inner.channel.send(value, permits).await
    }

    /// Closes the channel.
    /// All pending future send attempts will fail.
    /// Recv attempts will continue to succeed as long as there are items
    /// stored inside the channel. Further attempts will return `None`.
    pub fn close(&self) {
        self.inner.channel.close();
    }
}

impl<T> MpmcRx<T>
where
    T: Send + StableAbi,
{
    /// Returns a future that gets fulfilled when a value is written to the channel.
    /// If the channels gets closed, the future will resolve to `None`.
    pub async fn recv(&self) -> Option<T> {
        let fut = ChanRecv {
            channel: Some(self.inner.channel.clone()),
            wait_node: ListNode::new(RecvWaitQueueEntry::new()),
        };
        // Used to make the resulting future Send + Sync.
        pin_mut!(fut);
        fut.await
    }

    /// Closes the channel.
    /// All pending future send attempts will fail.
    /// Recv attempts will continue to succeed as long as there are items
    /// stored inside the channel. Further attempts will return `None`.
    pub fn close(&self) {
        self.inner.channel.close()
    }
}

impl<T> Stream for MpmcRx<T>
where
    T: Send + StableAbi,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let fut = ChanRecv {
            channel: Some(self.inner.channel.clone()),
            wait_node: ListNode::new(RecvWaitQueueEntry::new()),
        };
        pin_mut!(fut);
        fut.poll(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::{mpmc_chan, MpmcRx, MpmcTx, SendError};

    use abi_stable::{
        external_types::parking_lot::RMutex,
        std_types::{RArc, RHashMap},
        StableAbi,
    };
    use futures::future::{FusedFuture, Future, FutureExt};
    use futures::task::{Context, Poll};
    use futures_test::task::{new_count_waker, panic_waker};
    use futures_util::pin_mut;

    use std::fmt;

    #[derive(StableAbi, Debug)]
    #[repr(C)]
    struct DropCounterInner {
        count: RHashMap<usize, usize>,
    }

    #[derive(Clone, StableAbi)]
    #[repr(C)]
    struct DropCounter {
        inner: RArc<RMutex<DropCounterInner>>,
    }

    impl DropCounter {
        fn new() -> DropCounter {
            DropCounter {
                inner: RArc::new(RMutex::new(DropCounterInner {
                    count: RHashMap::new(),
                })),
            }
        }

        fn register_drop(&self, id: usize) {
            let mut guard = self.inner.lock();
            *guard.count.entry(id).or_insert(0) += 1;
        }

        fn clear(&self) {
            let mut guard = self.inner.lock();
            guard.count.clear();
        }

        fn drops(&self, id: usize) -> usize {
            let guard = self.inner.lock();
            *(guard.count.get(&id).unwrap_or(&0))
        }
    }

    impl fmt::Debug for DropCounter {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("DropCounter").finish()
        }
    }

    #[derive(Debug, PartialEq, StableAbi)]
    #[repr(C)]
    struct CountedElemInner {
        id: usize,
    }

    #[derive(Clone, StableAbi)]
    #[repr(C)]
    struct CountedElem {
        drop_counter: DropCounter,
        inner: RArc<RMutex<CountedElemInner>>,
    }

    impl fmt::Debug for CountedElem {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("CountedElem")
                .field("drop_counter", &self.drop_counter)
                .finish()
        }
    }

    impl PartialEq for CountedElem {
        fn eq(&self, other: &CountedElem) -> bool {
            self.id() == other.id()
        }
    }

    impl CountedElem {
        fn new(id: usize, drop_counter: DropCounter) -> CountedElem {
            CountedElem {
                inner: RArc::new(RMutex::new(CountedElemInner { id })),
                drop_counter,
            }
        }

        fn id(&self) -> usize {
            let guard = self.inner.lock();
            guard.id
        }

        fn strong_count(&self) -> usize {
            RArc::strong_count(&self.inner)
        }
    }

    impl Drop for CountedElem {
        fn drop(&mut self) {
            self.drop_counter.register_drop(self.id())
        }
    }

    fn assert_send_done<FutureType, T>(
        cx: &mut Context,
        send_fut: &mut core::pin::Pin<&mut FutureType>,
        expected: Result<(), SendError<T>>,
    ) where
        FutureType: Future<Output = Result<(), SendError<T>>> + FusedFuture,
        T: PartialEq + core::fmt::Debug,
    {
        match send_fut.as_mut().poll(cx) {
            Poll::Pending => panic!("future is not ready"),
            Poll::Ready(res) => {
                if res != expected {
                    panic!("Unexpected send result: {:?}", res);
                }
            }
        };
        assert!(send_fut.as_mut().is_terminated());
    }

    fn assert_send(cx: &mut Context, tx: &MpmcTx<i32>, value: i32, weight: usize) {
        let send_fut = tx.send(value, weight).fuse();
        pin_mut!(send_fut);
        assert!(!send_fut.as_mut().is_terminated());

        assert_send_done(cx, &mut send_fut, Ok(()));
    }

    fn assert_recv_done<FutureType, T>(
        cx: &mut Context,
        recv_fut: &mut core::pin::Pin<&mut FutureType>,
        value: Option<T>,
    ) where
        FutureType: Future<Output = Option<T>> + FusedFuture,
        T: PartialEq + core::fmt::Debug,
    {
        match recv_fut.as_mut().poll(cx) {
            Poll::Pending => panic!("future is not ready"),
            Poll::Ready(res) => {
                if res != value {
                    panic!("Unexpected value {:?}", res);
                }
            }
        };
        assert!(recv_fut.as_mut().is_terminated());
    }

    macro_rules! assert_recv {
        ($cx:ident, $channel:expr, $value: expr) => {
            let recv_fut = $channel.recv().fuse();
            pin_mut!(recv_fut);
            assert!(!recv_fut.as_mut().is_terminated());

            assert_recv_done($cx, &mut recv_fut, $value);
        };
    }

    #[test]
    fn send_on_closed_channel() {
        let (tx, rx) = mpmc_chan(0);
        let waker = &panic_waker();
        let cx = &mut Context::from_waker(&waker);

        drop(rx);
        let fut = tx.send(5, 0).fuse();
        pin_mut!(fut);
        assert_send_done(cx, &mut fut, Err(SendError(5)));
    }

    #[test]
    fn close_unblocks_send() {
        let (tx, rx) = mpmc_chan(0);
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let fut = tx.send(8, 1).fuse();
        pin_mut!(fut);
        assert!(fut.as_mut().poll(cx).is_pending());

        let fut2 = tx.send(9, 1).fuse();
        pin_mut!(fut2);
        assert!(fut2.as_mut().poll(cx).is_pending());

        assert_eq!(count, 0);

        // Dropping should wakeup the futures.
        drop(rx);
        assert_eq!(count, 2);
        assert_send_done(cx, &mut fut, Err(SendError(8)));
        assert_send_done(cx, &mut fut2, Err(SendError(9)));
    }

    #[test]
    fn close_unblocks_recv() {
        let (tx, rx) = mpmc_chan::<i32>(0);
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let fut = rx.recv().fuse();
        pin_mut!(fut);
        assert!(fut.as_mut().poll(cx).is_pending());

        let fut2 = rx.recv().fuse();
        pin_mut!(fut2);
        assert!(fut2.as_mut().poll(cx).is_pending());

        assert_eq!(count, 0);

        drop(tx);
        assert_eq!(count, 2);
        assert_recv_done(cx, &mut fut, None);
        assert_recv_done(cx, &mut fut2, None);
    }

    #[test]
    fn recv_after_send() {
        let (tx, rx) = mpmc_chan(0);
        let waker = &panic_waker();
        let cx = &mut Context::from_waker(&waker);

        assert_send(cx, &tx, 1, 0);
        assert_send(cx, &tx, 2, 0);
        assert_recv!(cx, &rx, Some(1));
        assert_recv!(cx, &rx, Some(2));

        assert_send(cx, &tx, 5, 0);
        assert_send(cx, &tx, 6, 0);
        assert_send(cx, &tx, 7, 0);
        tx.close();
        assert_recv!(cx, &rx, Some(5));
        assert_recv!(cx, &rx, Some(6));
        assert_recv!(cx, &rx, Some(7));
        assert_recv!(cx, &rx, None);
    }

    #[test]
    fn send_unblocks_recv() {
        let (tx, rx) = mpmc_chan(0);
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let fut = rx.recv().fuse();
        pin_mut!(fut);
        assert!(fut.as_mut().poll(cx).is_pending());
        assert_eq!(count, 0);

        let fut2 = rx.recv().fuse();
        pin_mut!(fut2);
        assert!(fut2.as_mut().poll(cx).is_pending());
        assert_eq!(count, 0);

        assert_send(cx, &tx, 99, 0);
        assert_eq!(count, 1);
        assert_recv_done(cx, &mut fut, Some(99));

        assert!(fut2.as_mut().poll(cx).is_pending());
        assert_send(cx, &tx, 111, 0);
        assert_eq!(count, 2);
        assert_recv_done(cx, &mut fut2, Some(111));
    }

    #[test]
    fn recv_unblocks_send() {
        let (tx, rx) = mpmc_chan(3);
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        // Fill the channel
        assert_send(cx, &tx, 1, 1);
        assert_send(cx, &tx, 2, 1);
        assert_send(cx, &tx, 3, 1);

        let fut = tx.send(4, 2).fuse();
        pin_mut!(fut);
        assert!(fut.as_mut().poll(cx).is_pending());

        let fut2 = tx.send(5, 1).fuse();
        pin_mut!(fut2);
        assert!(fut2.as_mut().poll(cx).is_pending());

        assert_eq!(count, 0);
        assert_recv!(cx, &rx, Some(1));
        assert_eq!(count, 0);
        assert_recv!(cx, &rx, Some(2));
        assert_eq!(count, 1);

        assert_send_done(cx, &mut fut, Ok(()));
        assert!(fut.is_terminated());
        assert!(fut2.as_mut().poll(cx).is_pending());

        assert_recv!(cx, &rx, Some(3));
        assert_eq!(count, 2);
        assert_send_done(cx, &mut fut2, Ok(()));
        assert!(fut2.is_terminated());
    }

    #[test]
    fn cancel_send_mid_wait() {
        let (tx, rx) = mpmc_chan(3);
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        assert_send(cx, &tx, 5, 1);
        assert_send(cx, &tx, 6, 1);
        assert_send(cx, &tx, 7, 1);

        {
            // Cancel a wait in between other waits
            // In order to arbitrarily drop a non movable future we have to box and pin it
            let mut poll1 = Box::pin(tx.send(8, 1).fuse());
            let mut poll2 = Box::pin(tx.send(9, 1).fuse());
            let mut poll3 = Box::pin(tx.send(10, 1).fuse());
            let mut poll4 = Box::pin(tx.send(11, 1).fuse());
            let mut poll5 = Box::pin(tx.send(12, 1).fuse());

            assert!(poll1.as_mut().poll(cx).is_pending());
            assert!(poll2.as_mut().poll(cx).is_pending());
            assert!(poll3.as_mut().poll(cx).is_pending());
            assert!(poll4.as_mut().poll(cx).is_pending());
            assert!(poll5.as_mut().poll(cx).is_pending());
            assert!(!poll1.is_terminated());
            assert!(!poll2.is_terminated());
            assert!(!poll3.is_terminated());
            assert!(!poll4.is_terminated());
            assert!(!poll5.is_terminated());

            // Cancel 2 futures. Only the remaining ones should get completed
            drop(poll2);
            drop(poll4);

            assert!(poll1.as_mut().poll(cx).is_pending());
            assert!(poll3.as_mut().poll(cx).is_pending());
            assert!(poll5.as_mut().poll(cx).is_pending());

            assert_recv!(cx, &rx, Some(5));
            assert_eq!(count, 1);
            assert_send_done(cx, &mut poll1.as_mut(), Ok(()));
            assert!(poll3.as_mut().poll(cx).is_pending());
            assert!(poll5.as_mut().poll(cx).is_pending());

            assert_recv!(cx, &rx, Some(6));
            assert_recv!(cx, &rx, Some(7));
            // We get only one wakeup at a time because we have to
            // actually receive the msg for the semaphore releaser to
            // be dropped.
            assert_eq!(count, 2);
            assert_send_done(cx, &mut poll3.as_mut(), Ok(()));
            assert_eq!(count, 3);
            assert_send_done(cx, &mut poll5.as_mut(), Ok(()));
        }
        assert_eq!(count, 3);
    }

    #[test]
    fn cancel_send_end_wait() {
        let (tx, rx) = mpmc_chan(3);
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        assert_send(cx, &tx, 100, 1);
        assert_send(cx, &tx, 101, 1);
        assert_send(cx, &tx, 102, 1);

        let poll1 = tx.send(1, 1).fuse();
        let poll2 = tx.send(2, 1).fuse();
        let poll3 = tx.send(3, 1).fuse();
        let poll4 = tx.send(4, 1).fuse();

        pin_mut!(poll1);
        pin_mut!(poll2);
        pin_mut!(poll3);
        pin_mut!(poll4);
        assert!(poll1.as_mut().poll(cx).is_pending());
        assert!(poll2.as_mut().poll(cx).is_pending());

        // Start polling some wait handles which get cancelled
        // before new ones are attached
        {
            let poll5 = tx.send(5, 1).fuse();
            let poll6 = tx.send(6, 1).fuse();
            pin_mut!(poll5);
            pin_mut!(poll6);
            assert!(poll5.as_mut().poll(cx).is_pending());
            assert!(poll6.as_mut().poll(cx).is_pending());
        }
        assert!(poll3.as_mut().poll(cx).is_pending());
        assert!(poll4.as_mut().poll(cx).is_pending());

        assert_recv!(cx, &rx, Some(100));
        assert_recv!(cx, &rx, Some(101));
        assert_recv!(cx, &rx, Some(102));

        assert_send_done(cx, &mut poll1, Ok(()));
        assert_send_done(cx, &mut poll2, Ok(()));
        assert_send_done(cx, &mut poll3, Ok(()));

        tx.close();
        assert_recv!(cx, &rx, Some(1));
        assert_recv!(cx, &rx, Some(2));
        assert_recv!(cx, &rx, Some(3));
        assert_send_done(cx, &mut poll4, Err(SendError(4)));

        assert_eq!(count, 4);
    }

    #[test]
    fn cancel_recv_mid_wait() {
        let (tx, rx) = mpmc_chan(3);
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        {
            let mut poll1 = Box::pin(rx.recv().fuse());
            let mut poll2 = Box::pin(rx.recv().fuse());
            let mut poll3 = Box::pin(rx.recv().fuse());
            let mut poll4 = Box::pin(rx.recv().fuse());
            let mut poll5 = Box::pin(rx.recv().fuse());

            assert!(poll1.as_mut().poll(cx).is_pending());
            assert!(poll2.as_mut().poll(cx).is_pending());
            assert!(poll3.as_mut().poll(cx).is_pending());
            assert!(poll4.as_mut().poll(cx).is_pending());
            assert!(poll5.as_mut().poll(cx).is_pending());
            assert!(!poll1.is_terminated());
            assert!(!poll2.is_terminated());
            assert!(!poll3.is_terminated());
            assert!(!poll4.is_terminated());
            assert!(!poll5.is_terminated());

            // Cancel 2 futures. Only the remaining ones should get completed
            drop(poll2);
            drop(poll4);

            assert!(poll1.as_mut().poll(cx).is_pending());
            assert!(poll3.as_mut().poll(cx).is_pending());
            assert!(poll5.as_mut().poll(cx).is_pending());

            assert_send(cx, &tx, 1, 1);
            assert_eq!(count, 1);
            assert_recv_done(cx, &mut poll1.as_mut(), Some(1));
            assert!(poll3.as_mut().poll(cx).is_pending());
            assert!(poll5.as_mut().poll(cx).is_pending());

            assert_send(cx, &tx, 2, 1);
            assert_send(cx, &tx, 3, 1);
            assert_eq!(count, 3);
            assert_recv_done(cx, &mut poll3.as_mut(), Some(2));
            assert_recv_done(cx, &mut poll5.as_mut(), Some(3));
        }

        assert_eq!(count, 3);
    }

    #[test]
    fn cancel_recv_end_wait() {
        let (tx, rx) = mpmc_chan(3);
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let poll1 = rx.recv().fuse();
        let poll2 = rx.recv().fuse();
        let poll3 = rx.recv().fuse();
        let poll4 = rx.recv().fuse();

        pin_mut!(poll1);
        pin_mut!(poll2);
        pin_mut!(poll3);
        pin_mut!(poll4);

        assert!(poll1.as_mut().poll(cx).is_pending());
        assert!(poll2.as_mut().poll(cx).is_pending());

        // Start polling some wait handles which get cancelled
        // before new ones are attached
        {
            let poll5 = rx.recv();
            let poll6 = rx.recv();
            pin_mut!(poll5);
            pin_mut!(poll6);
            assert!(poll5.as_mut().poll(cx).is_pending());
            assert!(poll6.as_mut().poll(cx).is_pending());
        }

        assert!(poll3.as_mut().poll(cx).is_pending());
        assert!(poll4.as_mut().poll(cx).is_pending());

        assert_send(cx, &tx, 0, 1);
        assert_send(cx, &tx, 1, 1);
        assert_send(cx, &tx, 2, 1);

        assert_recv_done(cx, &mut poll1, Some(0));
        assert_recv_done(cx, &mut poll2, Some(1));
        assert_recv_done(cx, &mut poll3, Some(2));

        assert_send(cx, &tx, 3, 1);
        assert_recv_done(cx, &mut poll4, Some(3));

        assert_eq!(count, 4);
    }

    #[test]
    fn drops_unread_elements() {
        let (waker, _) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let drop_counter = DropCounter::new();

        let elem1 = CountedElem::new(1, drop_counter.clone());
        let elem2 = CountedElem::new(2, drop_counter.clone());
        let elem3 = CountedElem::new(3, drop_counter.clone());

        {
            let (tx, rx) = mpmc_chan(3);

            // Fill the channel
            let fut1 = tx.send(elem1.clone(), 1).fuse();
            let fut2 = tx.send(elem2.clone(), 1).fuse();
            let fut3 = tx.send(elem3.clone(), 1).fuse();

            assert_eq!(2, elem1.strong_count());
            assert_eq!(2, elem2.strong_count());
            assert_eq!(2, elem3.strong_count());

            pin_mut!(fut1, fut2, fut3);
            assert_send_done(cx, &mut fut1, Ok(()));
            assert_send_done(cx, &mut fut2, Ok(()));
            assert_send_done(cx, &mut fut3, Ok(()));

            assert_eq!(2, elem1.strong_count());
            assert_eq!(2, elem2.strong_count());
            assert_eq!(2, elem3.strong_count());
        }

        assert_eq!(1, drop_counter.drops(1));
        assert_eq!(1, drop_counter.drops(2));
        assert_eq!(1, drop_counter.drops(3));

        assert_eq!(1, elem1.strong_count());
        assert_eq!(1, elem2.strong_count());
        assert_eq!(1, elem3.strong_count());

        drop_counter.clear();

        {
            let (tx, rx) = mpmc_chan(3);

            // Fill the channel
            let fut1 = tx.send(elem1.clone(), 1).fuse();
            let fut2 = tx.send(elem2.clone(), 1).fuse();

            let futr1 = rx.recv().fuse();
            let futr2 = rx.recv().fuse();
            pin_mut!(fut1, fut2, futr1, futr2);
            assert_send_done(cx, &mut fut1, Ok(()));
            assert_send_done(cx, &mut fut2, Ok(()));

            let fut3 = tx.send(elem3.clone(), 1).fuse();
            let fut4 = tx.send(elem2.clone(), 1).fuse();
            pin_mut!(fut3, fut4);
            assert_recv_done(cx, &mut futr1, Some(elem1.clone()));
            assert_recv_done(cx, &mut futr2, Some(elem2.clone()));

            assert_eq!(1, elem1.strong_count());
            assert_eq!(2, elem2.strong_count());

            assert_send_done(cx, &mut fut3, Ok(()));
            assert_send_done(cx, &mut fut4, Ok(()));

            assert_eq!(1, elem1.strong_count());
            assert_eq!(2, elem2.strong_count());
            assert_eq!(2, elem3.strong_count());

            // 1 and 2 are dropped twice, since we create a copy
            // through Option<T>
            assert_eq!(2, drop_counter.drops(1));
            assert_eq!(2, drop_counter.drops(2));
            assert_eq!(0, drop_counter.drops(3));

            drop_counter.clear();
        }

        assert_eq!(0, drop_counter.drops(1));
        assert_eq!(1, drop_counter.drops(2));
        assert_eq!(1, drop_counter.drops(3));

        assert_eq!(1, elem1.strong_count());
        assert_eq!(1, elem2.strong_count());
        assert_eq!(1, elem3.strong_count());
    }

    fn is_send<T: Send>(_: &T) {}

    fn is_send_value<T: Send>(_: T) {}

    fn is_sync<T: Sync>(_: &T) {}

    #[test]
    fn channel_futures_are_send() {
        let (tx, rx) = mpmc_chan(1);
        is_sync(&tx);
        is_sync(&rx);
        {
            let recv_fut = rx.recv();
            is_send(&recv_fut);
            pin_mut!(recv_fut);
            is_send(&recv_fut);
            let send_fut = tx.send(3, 1);
            is_send(&send_fut);
            pin_mut!(send_fut);
            is_send(&send_fut);
        }
        is_send_value(&tx);
        is_send_value(&rx);
    }
}
