/*
 * Copyright 2026 Ada Morgan, John Regan and the libev contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Module which contains all utilities for the libudp library.
//! These are used by libudp itself and can also be useful for the library user!
pub mod cache;
pub mod concurrent {
    //! Implementations for threading related functionality such as Thread-Factories.
    pub use libev::ext::{ExecutorCompletionService, ScheduledThreadPoolExecutor};
    pub use libev::utils::concurrent::{CountingThreadFactory, DelayedCompletableFuture, ThreadPerTaskExecutor};
    pub(crate) use libev::ext::{Dispatcher, ScheduledExecutorService};
}
pub(crate) mod config;
pub(crate) mod buffer;
pub(crate) use buffer::Buffer;
mod event_manager;
pub(crate) use event_manager::{EventManager, EventManagerProxy, EventQueue, cast};
mod interfaced_event_manager;
pub(crate) use interfaced_event_manager::InterfacedEventManager;
