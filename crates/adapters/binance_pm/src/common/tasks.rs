//! 异步任务句柄管理(等价于上游 binance crate 的私有 `TaskHandles`,
//! 该类型 `pub(crate)` 不可复用,故自建)。

use std::sync::Mutex;

use nautilus_common::live::get_runtime;
use tokio::task::JoinHandle;

/// 在途任务句柄集合(stop 时统一 abort,防泄漏)。
#[derive(Debug, Default)]
pub struct TaskHandles(Mutex<Vec<JoinHandle<()>>>);

impl TaskHandles {
    /// 在共享 runtime 上派生任务并登记句柄;任务返回 Err 时记日志。
    pub fn spawn<F>(&self, description: &'static str, fut: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let handle = get_runtime().spawn(async move {
            if let Err(e) = fut.await {
                log::error!("任务 {description} 失败:{e}");
            }
        });
        let mut guard = self.0.lock().expect("TaskHandles mutex poisoned");
        // 顺手清理已完成句柄,防无界增长。
        guard.retain(|h| !h.is_finished());
        guard.push(handle);
    }

    /// 中止全部在途任务。
    pub fn abort_all(&self) {
        let mut guard = self.0.lock().expect("TaskHandles mutex poisoned");
        for handle in guard.drain(..) {
            handle.abort();
        }
    }
}
