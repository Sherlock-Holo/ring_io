use std::sync::{Arc, Once, OnceLock, Weak};

use async_task::Runnable;
use rayon::ThreadPoolBuilder;

use crate::runtime::Task;

thread_local! {
    static THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
}

#[derive(Debug, Clone)]
enum Pool {
    Strong(Arc<rayon::ThreadPool>),
    Weak(Weak<rayon::ThreadPool>),
}

#[derive(Debug, Clone)]
pub struct ThreadPool {
    pool: Pool,
}

impl ThreadPool {
    pub fn new<F: Fn() + Send + Sync + 'static>(init: F) -> Self {
        let pool = Arc::new_cyclic(|pool| {
            let pool = pool.clone();

            ThreadPoolBuilder::new()
                .thread_name(|n| format!("ring_io_blocking_{n}"))
                .start_handler(move |_| {
                    init();

                    THREAD_POOL.with(|thread_pool| {
                        thread_pool
                            .set(ThreadPool {
                                pool: Pool::Weak(pool.clone()),
                            })
                            .unwrap_or_else(|_| unreachable!())
                    });
                })
                .build()
                .unwrap()
        });

        Self {
            pool: Pool::Strong(pool),
        }
    }

    pub fn spawn_blocking<T, F: FnOnce() -> T>(&self, f: F) -> Task<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let pool = match &self.pool {
            Pool::Strong(_) => {
                unreachable!("strong pool only in Runtime struct, won't use to spawn_blocking")
            }
            Pool::Weak(pool) => pool.upgrade().expect("ring_io runtime quit"),
        };

        let (runnable, task) = async_task::spawn(async move { f() }, move |runnable: Runnable| {
            let once = Once::new();

            once.call_once(|| {
                pool.spawn(|| {
                    runnable.run();
                });
            })
        });

        runnable.schedule();

        task
    }

    fn downgrade(&self) -> Self {
        let pool = match &self.pool {
            Pool::Strong(pool) => Arc::downgrade(pool),
            Pool::Weak(pool) => pool.clone(),
        };

        Self {
            pool: Pool::Weak(pool),
        }
    }
}

pub fn spawn_blocking<T, F: FnOnce() -> T>(f: F) -> Task<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let pool = THREAD_POOL.with(|pool| pool.get().expect("not in ring_io context").clone());

    pool.spawn_blocking(f)
}

pub fn set_thread_pool(thread_pool: ThreadPool) {
    THREAD_POOL.with(|pool| {
        pool.set(thread_pool.downgrade())
            .unwrap_or_else(|_| unreachable!("duplicate thread_pool set"))
    })
}
