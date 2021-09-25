use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

use io_uring::{CompletionQueue, IoUring, SubmissionQueue, Submitter};
use parking_lot::{Mutex, MutexGuard};

use crate::buffer::BufferManager;

pub struct DriverResource {
    ring: ManuallyDrop<IoUring>,

    // make sure only one thread can access io_uring sq
    sq_mutex: Mutex<()>,

    // use to set GroupBuffer available, or release the buffer
    buffer_manager: Mutex<BufferManager>,
}

impl DriverResource {
    pub fn new(ring: IoUring) -> Self {
        Self {
            ring: ManuallyDrop::new(ring),
            sq_mutex: Default::default(),
            buffer_manager: Mutex::new(BufferManager::new()),
        }
    }

    pub fn submission(&self) -> SubmissionRef {
        let _guard = self.sq_mutex.lock();

        // Safety: the sq_mutex protect it
        let sq = unsafe { self.ring.submission_shared() };

        SubmissionRef { sq, _guard }
    }

    #[inline]
    pub fn submitter(&self) -> Submitter {
        self.ring.submitter()
    }

    #[inline]
    pub unsafe fn completion(&self) -> CompletionQueue {
        self.ring.completion_shared()
    }

    #[inline]
    pub fn buffer_manager(&self) -> BufferManagerRef {
        BufferManagerRef {
            buffer_manager: self.buffer_manager.lock(),
        }
    }
}

pub struct SubmissionRef<'a> {
    sq: SubmissionQueue<'a>,
    _guard: MutexGuard<'a, ()>,
}

impl<'a> Deref for SubmissionRef<'a> {
    type Target = SubmissionQueue<'a>;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.sq
    }
}

impl<'a> DerefMut for SubmissionRef<'a> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sq
    }
}

pub struct BufferManagerRef<'a> {
    buffer_manager: MutexGuard<'a, BufferManager>,
}

impl<'a> Deref for BufferManagerRef<'a> {
    type Target = BufferManager;

    #[inline]
    fn deref(&self) -> &Self::Target {
        self.buffer_manager.deref()
    }
}

impl<'a> DerefMut for BufferManagerRef<'a> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.buffer_manager.deref_mut()
    }
}

/*impl Drop for DriverResource {
    fn drop(&mut self) {
        // Safety: we own the ring
        unsafe {
            ManuallyDrop::drop(&mut self.ring);
        }
    }
}*/
