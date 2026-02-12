use async_trait::async_trait;
pub mod l1_stg;
pub mod object_stg;

pub enum StateValue {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

#[async_trait]
pub trait StateBackend: Send + Sync + 'static {
    async fn get(&self, key: &[u8]) -> Option<StateValue>;
    async fn put(&self, key: &[u8], value: StateValue);
}
