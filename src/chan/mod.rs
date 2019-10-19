//! Asynchronous channels.
//!
//! This module provides various channels that can be used to communicate between
//! asynchronous tasks.

/// The error which is returned when sending a value into a channel fails.
///
/// The `send` operation can only fail if the channel has been closed, which
/// would prevent the other actors to ever retrieve the value.
///
/// The error recovers the value that has been sent.
#[derive(PartialEq, Debug)]
pub struct SendError<T>(pub T);

mod future;
use future::{ChanRecv, ChanSend};
use future::{
    ChanRecvAccess, ChanSendAccess, RecvPollState, RecvWaitQueueEntry, SendPollState,
    SendWaitQueueEntry,
};

mod once;

pub use once::*;

//mod oneshot_broadcast;
//
//pub use self::oneshot_broadcast::{
//    GenericOneshotBroadcastChannel,
//    LocalOneshotBroadcastChannel,
//};
//
//#[cfg(feature = "std")]
//pub use self::oneshot_broadcast::{
//    OneshotBroadcastChannel,
//};
//
//mod state_broadcast;
//pub use state_broadcast::{
//    StateId,
//    GenericStateBroadcastChannel,
//    LocalStateBroadcastChannel,
//    StateReceiveFuture,
//};
//
//#[cfg(feature = "std")]
//pub use self::state_broadcast::StateBroadcastChannel;
//

mod mpmc;

pub use mpmc::*;

//
//// The next section should really integrated if the alloc feature is active,
//// since it mainly requires `Arc` to be available. However for simplicity reasons
//// it is currently only activated in std environments.
//#[cfg(feature = "std")]
//mod if_alloc {
//
//    /// Channel implementations where Sender and Receiver sides are cloneable
//    /// and owned.
//    /// The Futures produced by channels in this module don't require a lifetime
//    /// parameter.
//    pub mod shared {
//        pub use super::super::channel_future::shared::*;
//        pub use super::super::mpmc::shared::*;
//        pub use super::super::oneshot_broadcast::shared::*;
//        pub use super::super::state_broadcast::shared::*;
//    }
//}
//
//#[cfg(feature = "std")]
//pub use self::if_alloc::*;
