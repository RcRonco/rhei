// rs-core/src/state.rs

use async_trait::async_trait;

// The User's View
#[async_trait]
pub trait StreamFunction: Send + Sync + 'static {
    type Input;
    type Output;

    // The user implements this.
    // Notice it is async! This allows them to await database calls.
    async fn process(&mut self, input: Self::Input, ctx: &mut StateContext) -> Vec<Self::Output>;
}

// The System's View (The "Context")
pub struct StateContext {
    // L1: Fast, synchronous access
    cache: L1Cache,
    // Access to the backend for misses
    backend: StateBackendHandle,
    // Current event time
    timestamp: u64,
}

impl StateContext {
    // Synchronous put (goes to L1)
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.cache.insert(key, value);
    }

    // Async get (might hit S3)
    pub async fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(v) = self.cache.get(key) {
            return Some(v.clone());
        }
        // Cache miss: Fetch from L2/L3
        self.backend.fetch(key).await
    }
}
