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

//! The IO handlers of each platform.
#[cfg_attr(any(target_os = "linux", target_os = "macos", target_os = "freebsd"), path = "sys/native.rs")]
#[cfg_attr(windows, path = "sys/win/mod.rs")]
mod platform;
pub(crate) use platform::*;
pub use libev::ext::CompletionService;
pub(crate) use libev::ext::{Completion, SocketHandler};
#[cfg(windows)]
pub(crate) use libev::ext::set_power_suppressed;
