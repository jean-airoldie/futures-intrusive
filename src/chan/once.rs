//! An asynchronously awaitable oneshot channel

use super::{ChanRecv, ChanRecvAccess, RecvPollState, RecvWaitQueueEntry, SendError};

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
use core::marker::PhantomData;
use futures_core::task::{Context, Poll};

unsafe fn wake_waiters(mut waiters: LinkedList<RecvWaitQueueEntry>) {
    // Reverse the waiter list, so that the oldest waker (which is
    // at the end of the list), gets woken first and has the best
    // chance to grab the channel value.
    waiters.reverse();

    for waiter in waiters.into_iter() {
        if let RSome(handle) = (*waiter).task.take() {
            handle.wake();
        }
        (*waiter).state = RecvPollState::Unregistered;
    }
}

/// Internal state of the oneshot channel with a stable ABI.
#[derive(StableAbi)]
#[repr(C)]
struct OnceState<T>
where
    T: StableAbi,
{
    /// Whether the channel had been fulfilled before
    is_fulfilled: bool,
    /// The value which is stored inside the channel
    value: ROption<T>,
    /// The list of waiters, which are waiting for the channel to get fulfilled
    waiters: LinkedList<RecvWaitQueueEntry>,
}

impl<T> OnceState<T>
where
    T: StableAbi,
{
    fn new() -> OnceState<T> {
        OnceState::<T> {
            is_fulfilled: false,
            value: RNone,
            waiters: LinkedList::new(),
        }
    }

    /// Writes a single value to the channel.
    /// If a value had been written to the channel before, the new value will be rejected.
    fn send(&mut self, value: T) -> Result<(), SendError<T>> {
        if self.is_fulfilled {
            return Err(SendError(value));
        }

        self.value = RSome(value);
        self.is_fulfilled = true;

        // Wakeup all waiters
        let waiters = self.waiters.take();
        // Safety: The linked list is guaranteed to be only manipulated inside
        // the mutex in scope of the ChanState and is thereby guaranteed to
        // be consistent.
        unsafe {
            wake_waiters(waiters);
        }

        Ok(())
    }

    fn close(&mut self) {
        if self.is_fulfilled {
            return;
        }
        self.is_fulfilled = true;

        // Wakeup all waiters
        let waiters = self.waiters.take();
        // Safety: The linked list is guaranteed to be only manipulated inside
        // the mutex in scope of the ChanState and is thereby guaranteed to
        // be consistent.
        unsafe {
            wake_waiters(waiters);
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
            RecvPollState::Unregistered => {
                let maybe_val = self.value.take();
                match maybe_val {
                    RSome(v) => {
                        // A value was available inside the channel and was fetched
                        Poll::Ready(Some(v))
                    }
                    RNone => {
                        // Check if something was written into the channel before
                        // or the channel was closed.
                        if self.is_fulfilled {
                            Poll::Ready(None)
                        } else {
                            // Added the task to the wait queue
                            wait_node.task = RSome(RWaker::new(cx.waker().clone()));
                            wait_node.state = RecvPollState::Registered;
                            self.waiters.add_front(wait_node);
                            Poll::Pending
                        }
                    }
                }
            }
            RecvPollState::Registered => {
                // Since the channel wakes up all waiters and moves their states to unregistered
                // there can't be any value in the channel in this state.
                Poll::Pending
            }
            RecvPollState::Notified => {
                unreachable!("Not possible for Once");
            }
        }
    }

    fn remove_waiter(&mut self, wait_node: &mut ListNode<RecvWaitQueueEntry>) {
        // ChanRecvFuture only needs to get removed if it had been added to
        // the wait queue of the channel. This has happened in the RecvPollState::Waiting case.
        if let RecvPollState::Registered = wait_node.state {
            if !unsafe { self.waiters.remove(wait_node) } {
                // Panic if the address isn't found. This can only happen if the contract was
                // violated, e.g. the RecvWaitQueueEntry got moved after the initial poll.
                panic!("Future could not be removed from wait queue");
            }
            wait_node.state = RecvPollState::Unregistered;
        }
    }
}

/// A channel with a stable ABI which can be used to exchange a single value
/// between two concurrent tasks.
///
/// Tasks can wait for the value to get delivered via `recv`.
/// The returned Future will get fulfilled when a value is sent into the channel.
///
/// The value can only be extracted by a single receiving task. Once the value
/// has been retrieved from the Chan, the Chan is closed and subsequent
/// receive calls will return `None`.
#[derive(StableAbi)]
#[repr(C)]
pub struct OnceChan<T>
where
    T: StableAbi,
{
    inner: RArc<RMutex<OnceState<T>>>,
}

impl<T> Clone for OnceChan<T>
where
    T: StableAbi,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

// The channel can be sent to other threads as long as it's not borrowed and the
// value in it can be sent to other threads.
unsafe impl<T> Send for OnceChan<T> where T: Send + StableAbi {}

// The channel is thread-safe as long as a thread-safe mutex is used
unsafe impl<T> Sync for OnceChan<T> where T: Send + StableAbi {}

impl<T> core::fmt::Debug for OnceChan<T>
where
    T: StableAbi,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("OnceChan").finish()
    }
}

impl<T> OnceChan<T>
where
    T: StableAbi,
{
    /// Creates a new OnceChan in the given state
    fn new() -> Self {
        OnceChan {
            inner: RArc::new(RMutex::new(OnceState::new())),
        }
    }

    /// Writes a single value to the channel.
    ///
    /// This will notify waiters about the availability of the value.
    /// If a value had been written to the channel before, or if the
    /// channel is closed, the new value will be rejected and
    /// returned inside the error variant.
    fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.inner.lock().send(value)
    }

    /// Closes the channel.
    ///
    /// This will notify waiters about closure, by fulfilling pending `Future`s
    /// with `None`.
    /// `send(value)` attempts which follow this call will fail with a
    /// [`SendError`].
    fn close(&self) {
        self.inner.lock().close()
    }
}

impl<T> ChanRecvAccess for OnceChan<T>
where
    T: StableAbi + Send,
{
    type Item = T;

    unsafe fn try_recv(
        &self,
        wait_node: &mut ListNode<RecvWaitQueueEntry>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<T>> {
        self.inner.lock().try_recv(wait_node, cx)
    }

    fn remove_recv_waiter(&self, wait_node: &mut ListNode<RecvWaitQueueEntry>) {
        self.inner.lock().remove_waiter(wait_node)
    }
}

/// The sending side of a channel which can be used to exchange values
/// between concurrent tasks.
///
/// Values can be sent into the channel through `send`.
pub struct OnceTx<T>
where
    T: StableAbi,
{
    inner: OnceChan<T>,
}

impl<T> OnceTx<T>
where
    T: StableAbi,
{
    /// Writes a single value to the channel.
    ///
    /// This will notify waiters about the availability of the value.
    /// If a value had been written to the channel before, or if the
    /// channel is closed, the new value will be rejected and
    /// returned inside the error variant.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.inner.send(value)
    }
}

impl<T> core::fmt::Debug for OnceTx<T>
where
    T: StableAbi,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("OnceTx").finish()
    }
}

impl<T> Drop for OnceTx<T>
where
    T: StableAbi,
{
    fn drop(&mut self) {
        // Close the channel, before last sender gets destroyed
        // TODO: We could potentially avoid this, if no receiver is left
        self.inner.close();
    }
}

/// The receiving side of a channel which can be used to exchange values
/// between concurrent tasks.
///
/// Tasks can receive values from the channel through the `receive` method.
/// The returned Future will get resolved when a value is sent into the channel.
pub struct OnceRx<T>
where
    T: StableAbi,
{
    inner: OnceChan<T>,
}

impl<T> OnceRx<T>
where
    T: StableAbi + Send,
{
    /// Returns a future that gets fulfilled when a value is written to the channel.
    /// If the channels gets closed, the future will resolve to `None`.
    pub fn recv(&self) -> ChanRecv<OnceChan<T>> {
        ChanRecv {
            channel: Some(self.inner.clone()),
            wait_node: ListNode::new(RecvWaitQueueEntry::new()),
        }
    }
}

impl<T> core::fmt::Debug for OnceRx<T>
where
    T: StableAbi,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("OnceRx").finish()
    }
}

impl<T> Drop for OnceRx<T>
where
    T: StableAbi,
{
    fn drop(&mut self) {
        // Close the channel, before last receiver gets destroyed
        // TODO: We could potentially avoid this, if no sender is left
        self.inner.close();
    }
}

/// Creates a new oneshot channel which can be used to exchange values
/// of type `T` between concurrent tasks.
/// The ends of the Chan are represented through
/// the returned Sender and Recvr.
///
/// As soon es either the senders or receivers is closed, the channel
/// itself will be closed.
///
/// Example for creating a channel to transmit an integer value:
///
/// ```
/// # use futures_intrusive::channel::shared::oneshot_channel;
/// let (sender, receiver) = oneshot_channel::<i32>();
/// ```
pub fn once_chan<T>() -> (OnceTx<T>, OnceRx<T>)
where
    T: Send + StableAbi,
{
    let inner = OnceChan::new();

    let sender = OnceTx {
        inner: inner.clone(),
    };
    let receiver = OnceRx { inner };

    (sender, receiver)
}

#[cfg(test)]
mod tests {
    use super::{once_chan, SendError};
    use futures::future::{FusedFuture, Future};
    use futures::task::{Context, Poll};
    use futures_test::task::{new_count_waker, panic_waker};
    use futures_util::pin_mut;

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

    #[test]
    fn send_on_closed_channel() {
        let (tx, rx) = once_chan::<i32>();
        drop(rx);
        let err = tx.send(5).unwrap_err();
        assert_eq!(SendError(5), err);
    }

    #[test]
    fn close_unblocks_recv() {
        let (tx, rx) = once_chan::<i32>();
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let fut = rx.recv();
        pin_mut!(fut);
        assert!(fut.as_mut().poll(cx).is_pending());
        let fut2 = rx.recv();
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
        let (tx, rx) = once_chan::<i32>();
        let waker = &panic_waker();
        let cx = &mut Context::from_waker(&waker);

        tx.send(5).unwrap();

        let recv_fut = rx.recv();
        pin_mut!(recv_fut);
        assert!(!recv_fut.as_mut().is_terminated());

        assert_recv_done(cx, &mut recv_fut, Some(5));

        // A second recv attempt must yield None, since the
        // value was taken out of the channel
        let recv_fut2 = rx.recv();
        pin_mut!(recv_fut2);
        assert_recv_done(cx, &mut recv_fut2, None);
    }

    #[test]
    fn send_after_recv() {
        let (tx, rx) = once_chan::<i32>();
        let (waker, _) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let recv_fut1 = rx.recv();
        let recv_fut2 = rx.recv();
        pin_mut!(recv_fut1);
        pin_mut!(recv_fut2);
        assert!(!recv_fut1.as_mut().is_terminated());
        assert!(!recv_fut2.as_mut().is_terminated());

        let poll_res1 = recv_fut1.as_mut().poll(cx);
        let poll_res2 = recv_fut2.as_mut().poll(cx);
        assert!(poll_res1.is_pending());
        assert!(poll_res2.is_pending());

        tx.send(5).unwrap();

        assert_recv_done(cx, &mut recv_fut1, Some(5));
        // recv_fut2 isn't terminated, since it hasn't been polled
        assert!(!recv_fut2.as_mut().is_terminated());
        // When it gets polled, it must evaluate to None
        assert_recv_done(cx, &mut recv_fut2, None);
    }

    #[test]
    fn second_send_rejects_value() {
        let (tx, rx) = once_chan::<i32>();
        let (waker, _) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let recv_fut1 = rx.recv();
        pin_mut!(recv_fut1);
        assert!(!recv_fut1.as_mut().is_terminated());
        assert!(recv_fut1.as_mut().poll(cx).is_pending());

        // First send
        tx.send(5).unwrap();

        assert!(recv_fut1.as_mut().poll(cx).is_ready());

        // Second send
        let send_res = tx.send(7);
        match send_res {
            Err(SendError(7)) => {} // expected
            _ => panic!("Second second should reject"),
        }
    }

    #[test]
    fn cancel_mid_wait() {
        let (tx, rx) = once_chan();
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        {
            // Cancel a wait in between other waits
            // In order to arbitrarily drop a non movable future we have to box and pin it
            let mut poll1 = Box::pin(rx.recv());
            let mut poll2 = Box::pin(rx.recv());
            let mut poll3 = Box::pin(rx.recv());
            let mut poll4 = Box::pin(rx.recv());
            let mut poll5 = Box::pin(rx.recv());

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

            assert_eq!(count, 0);
            tx.send(7).unwrap();
            assert_eq!(count, 3);

            assert!(poll1.as_mut().poll(cx).is_ready());
            assert!(poll3.as_mut().poll(cx).is_ready());
            assert!(poll5.as_mut().poll(cx).is_ready());
            assert!(poll1.is_terminated());
            assert!(poll3.is_terminated());
            assert!(poll5.is_terminated());
        }

        assert_eq!(count, 3)
    }

    #[test]
    fn cancel_end_wait() {
        let (tx, rx) = once_chan();
        let (waker, count) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let poll1 = rx.recv();
        let poll2 = rx.recv();
        let poll3 = rx.recv();
        let poll4 = rx.recv();

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

        tx.send(99).unwrap();

        assert!(poll1.as_mut().poll(cx).is_ready());
        assert!(poll2.as_mut().poll(cx).is_ready());
        assert!(poll3.as_mut().poll(cx).is_ready());
        assert!(poll4.as_mut().poll(cx).is_ready());

        assert_eq!(count, 4)
    }

    fn is_send<T: Send>(_: &T) {}

    fn is_send_value<T: Send>(_: T) {}

    fn is_sync<T: Sync>(_: &T) {}

    #[test]
    fn channel_futures_are_send() {
        let (tx, rx) = once_chan();
        is_sync(&tx);
        is_sync(&rx);
        {
            let recv_fut = rx.recv();
            is_send(&recv_fut);
            pin_mut!(recv_fut);
            is_send(&recv_fut);
            let send_fut = tx.send(3);
            is_send(&send_fut);
            pin_mut!(send_fut);
            is_send(&send_fut);
        }
        is_send_value(tx);
        is_send_value(rx);
    }

    #[test]
    fn dropping_shared_channel_txs_closes_channel() {
        let (waker, _) = new_count_waker();
        let cx = &mut Context::from_waker(&waker);

        let (tx, rx) = once_chan::<i32>();

        let fut = rx.recv();
        pin_mut!(fut);
        assert!(fut.as_mut().poll(cx).is_pending());

        drop(tx);

        match fut.as_mut().poll(cx) {
            Poll::Ready(None) => {}
            Poll::Ready(Some(_)) => panic!("Expected no value"),
            Poll::Pending => panic!("Expected channel to be closed"),
        }
    }
}
