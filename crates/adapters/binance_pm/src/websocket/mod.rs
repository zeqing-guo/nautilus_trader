//! PM 用户数据流(`wss://…/pm/ws/<listenKey>`):事件模型与分发。
//!
//! 后续切片:client(连接/24h 轮换)、handler、recovery(listenKey 生命周期 +
//! 死流检测——fstream 对任意路径都 101,握手成功不等于流健康)。

pub mod messages;
