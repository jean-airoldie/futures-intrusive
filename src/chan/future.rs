use crate::{intrusive_singly_linked_list::ListNode, waker::RWaker};

use super::SendError;

use {
    abi_stable::{
        std_types::{
            RBox,
            ROption::{self, *},
        },
        StableAbi,
    },
    futures_core::{
        future::{FusedFuture, Future},
        task::{Context, Poll, RawWaker, Waker},
    },
};

use core::{marker::PhantomData, pin::Pin};
use std::{mem, slice, sync::Arc};

/// Tracks how the future had interacted with the channel
#[derive(PartialEq, Debug, StableAbi)]
#[repr(u8)]
pub enum RecvPollState {
    /// The task is not registered at the wait queue at the channel
    Unregistered,
    /// The task was added to the wait queue at the channel.
    Registered,
    /// The task was notified that a value is available or can be sent,
    /// but hasn't interacted with the channel since then
    Notified,
}

/// Tracks the channel futures waiting state.
/// Access to this struct is synchronized through the channel.
#[derive(Debug, StableAbi)]
#[repr(C)]
pub struct RecvWaitQueueEntry {
    /// The task handle of the waiting task
    pub task: ROption<RWaker>,
    /// Current polling state
    pub state: RecvPollState,
}

impl RecvWaitQueueEntry {
    /// Creates a new RecvWaitQueueEntry
    pub fn new() -> RecvWaitQueueEntry {
        RecvWaitQueueEntry {
            task: ROption::RNone,
            state: RecvPollState::Unregistered,
        }
    }
}

/// Tracks how the future had interacted with the channel
#[derive(PartialEq, Debug, StableAbi)]
#[repr(u8)]
pub enum SendPollState {
    /// The task is not registered at the wait queue at the channel
    Unregistered,
    /// The task was added to the wait queue at the channel.
    Registered,
    /// The value has been transmitted to the other task
    SendComplete,
}

/// Tracks the channel futures waiting state.
/// Access to this struct is synchronized through the channel.
#[derive(StableAbi)]
#[repr(C)]
pub struct SendWaitQueueEntry<T>
where
    T: StableAbi,
{
    /// The task handle of the waiting task
    pub task: ROption<RWaker>,
    /// Current polling state
    pub state: SendPollState,
    /// The value to send
    pub value: ROption<T>,
}

impl<T> core::fmt::Debug for SendWaitQueueEntry<T>
where
    T: StableAbi,
{
    fn fmt(
        &self,
        fmt: &mut core::fmt::Formatter<'_>,
    ) -> core::result::Result<(), core::fmt::Error> {
        fmt.debug_struct("SendWaitQueueEntry")
            .field("task", &self.task)
            .field("state", &self.state)
            .finish()
    }
}

impl<T> SendWaitQueueEntry<T>
where
    T: StableAbi,
{
    /// Creates a new SendWaitQueueEntry
    pub fn new(value: T) -> SendWaitQueueEntry<T> {
        SendWaitQueueEntry {
            task: RNone,
            state: SendPollState::Unregistered,
            value: RSome(value),
        }
    }
}

pub trait ChanSendAccess {
    type Item: Send + StableAbi;

    unsafe fn try_send(
        &self,
        wait_node: &mut ListNode<SendWaitQueueEntry<Self::Item>>,
        cx: &mut Context<'_>,
    ) -> (Poll<()>, Option<Self::Item>);

    fn remove_send_waiter(&self, wait_node: &mut ListNode<SendWaitQueueEntry<Self::Item>>);
}

pub trait ChanRecvAccess {
    type Item: Send;

    unsafe fn try_recv(
        &self,
        wait_node: &mut ListNode<RecvWaitQueueEntry>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>>;

    fn remove_recv_waiter(&self, wait_node: &mut ListNode<RecvWaitQueueEntry>);
}

/// A Future that is returned by the `recv` function on a channel.
/// The future gets resolved with `Some(value)` when a value could be
/// received from the channel.
/// If the channels gets closed and no items are still enqueued inside the
/// channel, the future will resolve to `None`.
#[must_use = "futures do nothing unless polled"]
pub struct ChanRecv<T>
where
    T: ChanRecvAccess,
{
    /// The Chan that is associated with this ChanRecv
    pub(crate) channel: Option<T>,
    /// Node for waiting on the channel
    pub(crate) wait_node: ListNode<RecvWaitQueueEntry>,
}

// Safety: Chan futures can be sent between threads as long as the underlying
// channel is thread-safe (Sync), which allows to poll/register/unregister from
// a different thread.
unsafe impl<T: ChanRecvAccess> Send for ChanRecv<T> {}

impl<T> core::fmt::Debug for ChanRecv<T>
where
    T: ChanRecvAccess,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("ChanRecv").finish()
    }
}

impl<T> Future for ChanRecv<T>
where
    T: ChanRecvAccess,
{
    type Output = Option<T::Item>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T::Item>> {
        // It might be possible to use Pin::map_unchecked here instead of the two unsafe APIs.
        // However this didn't seem to work for some borrow checker reasons

        // Safety: The next operations are safe, because Pin promises us that
        // the address of the wait queue entry inside ChanRecv is stable,
        // and we don't move any fields inside the future until it gets dropped.
        let mut_self: &mut ChanRecv<T> = unsafe { Pin::get_unchecked_mut(self) };

        let channel = mut_self
            .channel
            .take()
            .expect("polled ChanRecv after completion");

        let poll_res = unsafe { channel.try_recv(&mut mut_self.wait_node, cx) };

        if poll_res.is_ready() {
            // A value was available
            mut_self.channel = None;
        } else {
            mut_self.channel = Some(channel)
        }

        poll_res
    }
}

impl<T> FusedFuture for ChanRecv<T>
where
    T: ChanRecvAccess,
{
    fn is_terminated(&self) -> bool {
        self.channel.is_none()
    }
}

impl<T> Drop for ChanRecv<T>
where
    T: ChanRecvAccess,
{
    fn drop(&mut self) {
        // If this ChanRecvFuture has been polled and it was added to the
        // wait queue at the channel, it must be removed before dropping.
        // Otherwise the channel would access invalid memory.
        if let Some(channel) = &self.channel {
            channel.remove_recv_waiter(&mut self.wait_node);
        }
    }
}

/// A Future that is returned by the `send` function on a channel.
/// The future gets resolved with `None` when a value could be
/// written to the channel.
/// If the channel gets closed the send operation will fail, and the
/// Future will resolve to `SendError(T)` and return the item
/// to send.
#[must_use = "futures do nothing unless polled"]
pub struct ChanSend<T>
where
    T: ChanSendAccess,
{
    /// The Chan that is associated with this ChanSend
    pub(crate) channel: Option<Arc<T>>,
    /// Node for waiting on the channel
    pub(crate) wait_node: ListNode<SendWaitQueueEntry<T::Item>>,
}

// Safety: Chan futures can be sent between threads as long as the underlying
// channel is thread-safe (Sync), which allows to poll/register/unregister from
// a different thread.
unsafe impl<T: ChanSendAccess + Send> Send for ChanSend<T> {}

impl<T> core::fmt::Debug for ChanSend<T>
where
    T: ChanSendAccess,
{
    fn fmt(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
        f.debug_struct("ChanSend").finish()
    }
}

impl<T> Future for ChanSend<T>
where
    T: ChanSendAccess,
{
    type Output = Result<(), SendError<T::Item>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), SendError<T::Item>>> {
        // It might be possible to use Pin::map_unchecked here instead of the two unsafe APIs.
        // However this didn't seem to work for some borrow checker reasons

        // Safety: The next operations are safe, because Pin promises us that
        // the address of the wait queue entry inside ChanSend is stable,
        // and we don't move any fields inside the future until it gets dropped.
        let mut_self: &mut ChanSend<T> = unsafe { Pin::get_unchecked_mut(self) };

        let channel = mut_self
            .channel
            .take()
            .expect("polled ChanSend after completion");

        let send_res = unsafe { channel.try_send(&mut mut_self.wait_node, cx) };

        match send_res.0 {
            Poll::Ready(()) => {
                // Value has been transmitted or channel was closed
                match send_res.1 {
                    Some(v) => {
                        // Chan must have been closed
                        Poll::Ready(Err(SendError(v)))
                    }
                    None => Poll::Ready(Ok(())),
                }
            }
            Poll::Pending => {
                mut_self.channel = Some(channel);
                Poll::Pending
            }
        }
    }
}

impl<T> FusedFuture for ChanSend<T>
where
    T: ChanSendAccess,
{
    fn is_terminated(&self) -> bool {
        self.channel.is_none()
    }
}

impl<T> Drop for ChanSend<T>
where
    T: ChanSendAccess,
{
    fn drop(&mut self) {
        // If this ChanSend has been polled and it was added to the
        // wait queue at the channel, it must be removed before dropping.
        // Otherwise the channel would access invalid memory.
        if let Some(channel) = &self.channel {
            channel.remove_send_waiter(&mut self.wait_node);
        }
    }
}
