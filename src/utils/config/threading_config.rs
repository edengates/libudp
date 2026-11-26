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

use crate::utils::concurrent::ScheduledThreadPoolExecutor;
use std::io;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct ThreadingConfig {
    pub(crate) socket_pool: ScheduledThreadPoolExecutor,
    pub(crate) buffer_size: usize,
    pub(crate) threshold: usize,
    pub(crate) large_threshold: usize,
    pub(crate) shutdown_timeout: Duration,
    pub(crate) chunk_limit: usize,
    pub(crate) packet_limit: usize,
    pub(crate) is_power_suppressed_enabled: bool,
}

impl ThreadingConfig {
    pub fn socket_pool(&self) -> &ScheduledThreadPoolExecutor {
        &self.socket_pool
    }

    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    pub fn threshold(&self) -> usize {
        self.threshold
    }

    pub fn large_threshold(&self) -> usize {
        self.large_threshold
    }

    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    pub fn chunk_limit(&self) -> usize {
        self.chunk_limit
    }

    pub fn packet_limit(&self) -> usize {
        self.packet_limit
    }

    pub fn is_power_suppressed_enabled(&self) -> bool {
        self.is_power_suppressed_enabled
    }

    pub fn shutdown(&self) {
        for shard in self.socket_pool.shards() {
            shard.shutdown(self.shutdown_timeout);
        }
    }

    pub fn await_shutdown(&self, timeout: Duration) -> bool {
        let deadline = Instant::now().checked_add(timeout);
        self.socket_pool
            .shards()
            .iter()
            .all(|shard| shard.await_shutdown(deadline.map_or(timeout, |deadline| deadline.saturating_duration_since(Instant::now()))))
    }

    pub fn shutdown_now(&self) -> io::Result<()> {
        let mut data = Ok(());
        for shard in self.socket_pool.shards() {
            data = data.and(shard.shutdown_now());
        }
        data
    }
}
