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

use crate::entities::channel::concrete::ThreadChannel;
use rustc_hash::FxHashMap;
use std::any::Any;
use std::sync::{PoisonError, RwLock, RwLockReadGuard};

/// Read-only view on internal libev cache of channels.
///
/// This can be useful to check information such as size without creating an immutable snapshot first.
///
/// The cache holds the channel of the last listener of each connection, `N` and `P` are its message types.
#[derive(Default)]
pub struct ChannelCacheView {
    channels: RwLock<FxHashMap<u64, Box<dyn Any + Send + Sync>>>,
}

impl ChannelCacheView {
    /// Creates an immutable snapshot of the current cache state.
    pub fn to_vec<N: 'static, P: 'static>(&self) -> Vec<ThreadChannel<N, P>> {
        self.read().values().filter_map(|channel| channel.downcast_ref().cloned()).collect()
    }

    /// Retrieves the entity represented by the provided ID.
    pub fn get_element_by_id<N: 'static, P: 'static>(&self, id: u64) -> Option<ThreadChannel<N, P>> {
        self.read().get(&id).and_then(|channel| channel.downcast_ref().cloned())
    }

    /// Performs the given action for each element of the cache until all elements have been processed.
    pub fn for_each<N: 'static, P: 'static, F: FnMut(&ThreadChannel<N, P>)>(&self, action: F) {
        self.read().values().filter_map(|channel| channel.downcast_ref()).for_each(action);
    }

    /// The current size of this cache.
    pub fn len(&self) -> usize {
        self.read().len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.read().is_empty()
    }

    pub(crate) fn add(&self, id: u64, channel: Box<dyn Any + Send + Sync>) {
        self.channels.write().unwrap_or_else(PoisonError::into_inner).insert(id, channel);
    }

    pub(crate) fn remove(&self, id: u64) {
        self.channels.write().unwrap_or_else(PoisonError::into_inner).remove(&id);
    }

    fn read(&self) -> RwLockReadGuard<'_, FxHashMap<u64, Box<dyn Any + Send + Sync>>> {
        self.channels.read().unwrap_or_else(PoisonError::into_inner)
    }
}
