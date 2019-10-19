//! Asynchronous synchronization primitives based on intrusive collections.
//!
//! This module provides various primitives for synchronizing concurrently
//! executing futures.

//mod mutex;
//
//pub use self::mutex::{
//    GenericMutex, GenericMutexLockFuture, GenericMutexGuard,
//    LocalMutex, LocalMutexLockFuture, LocalMutexGuard,
//};
//
//#[cfg(feature = "std")]
//pub use self::mutex::{
//    Mutex, MutexLockFuture, MutexGuard,
//};

mod semaphore;

pub use self::semaphore::{Releaser, Semaphore, SemaphoreAcquire};
