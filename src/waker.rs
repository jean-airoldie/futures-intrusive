use {
    abi_stable::{
        sabi_extern_fn,
        std_types::ROption::{self, *},
        StableAbi,
    },
    futures_core::task::Waker,
};

use std::fmt;

#[derive(StableAbi, Debug)]
#[repr(C)]
pub struct OpaqueWaker {
    #[sabi(unsafe_opaque_field)]
    inner: Waker,
}

#[sabi_extern_fn]
fn wake_(waker: OpaqueWaker) {
    waker.inner.wake()
}

#[sabi_extern_fn]
fn wake_by_ref_(waker: &OpaqueWaker) {
    waker.inner.wake_by_ref()
}

#[sabi_extern_fn]
fn drop_(waker: OpaqueWaker) {
    drop(waker.inner)
}

#[sabi_extern_fn]
fn clone_(waker: &OpaqueWaker) -> OpaqueWaker {
    OpaqueWaker {
        inner: waker.inner.clone(),
    }
}

#[derive(StableAbi)]
#[repr(C)]
pub struct RWaker {
    waker: ROption<OpaqueWaker>,
    wake: unsafe extern "C" fn(OpaqueWaker),
    wake_by_ref: unsafe extern "C" fn(&OpaqueWaker),
    drop: unsafe extern "C" fn(OpaqueWaker),
    clone: unsafe extern "C" fn(&OpaqueWaker) -> OpaqueWaker,
}

impl RWaker {
    pub fn new(waker: Waker) -> Self {
        let waker = RSome(OpaqueWaker { inner: waker });

        Self {
            waker,
            wake: wake_,
            wake_by_ref: wake_by_ref_,
            drop: drop_,
            clone: clone_,
        }
    }

    #[inline]
    pub fn wake(mut self) {
        unsafe { (self.wake)(self.waker.take().unwrap()) }
    }

    #[inline]
    pub fn wake_by_ref(&self) {
        unsafe { (self.wake_by_ref)(self.waker.as_ref().unwrap()) }
    }
}

impl Drop for RWaker {
    #[inline]
    fn drop(&mut self) {
        // Drop the waker.
        if let RSome(waker) = self.waker.take() {
            unsafe { (self.drop)(waker) };
        }

        // The rest is dropped by the `RWaker`.
    }
}

impl Clone for RWaker {
    #[inline]
    fn clone(&self) -> Self {
        let waker = RSome(unsafe { (self.clone)(self.waker.as_ref().unwrap()) });

        RWaker {
            waker,
            wake: self.wake,
            wake_by_ref: self.wake_by_ref,
            drop: self.drop,
            clone: self.clone,
        }
    }
}

impl fmt::Debug for RWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RWaker")
            .field("waker", &self.waker)
            .finish()
    }
}
