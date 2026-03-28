//! Cloneable, type-erased wrapper for pipeline elements.
//!
//! All values flowing through the Timely dataflow are wrapped in [`AnyItem`].
//! This module also contains the global type registry for cross-process
//! serialization via bincode.

use std::any::Any;
use std::sync::{LazyLock, RwLock};

// ── Cloneable type-erased wrapper ────────────────────────────────────

/// Object-safe trait for cloneable, type-erased values.
///
/// Implemented automatically for all `T: Clone + Any + Send + Debug`. This enables
/// `AnyItem` to genuinely clone its contents, which is required for Timely's
/// `Exchange` pact (used in distributed execution).
pub(crate) trait CloneAnySend: Any + Send {
    /// Clone the value into a new boxed trait object.
    fn clone_box(&self) -> Box<dyn CloneAnySend>;
    /// Convert to `Box<dyn Any + Send>` for downcasting.
    fn into_any(self: Box<Self>) -> Box<dyn Any + Send>;
    /// Borrow as `&dyn Any` for type checking and ref downcasting.
    fn as_any_ref(&self) -> &dyn Any;
    /// Best-effort debug representation for DLQ diagnostics.
    fn debug_repr(&self) -> String;
    /// Stable hash of the concrete type name, used as a serialization tag.
    fn stable_type_id(&self) -> u64;
    /// Serialize the value to bytes via bincode.
    fn serialize_bytes(&self) -> Vec<u8>;
}

impl<T: Clone + Any + Send + std::fmt::Debug + serde::Serialize> CloneAnySend for T {
    fn clone_box(&self) -> Box<dyn CloneAnySend> {
        Box::new(self.clone())
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any + Send> {
        self
    }

    fn as_any_ref(&self) -> &dyn Any {
        self
    }

    fn debug_repr(&self) -> String {
        format!("{self:?}")
    }

    fn stable_type_id(&self) -> u64 {
        seahash::hash(std::any::type_name::<T>().as_bytes())
    }

    fn serialize_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("AnyItem serialization failed")
    }
}

// ── Global type registry for AnyItem deserialization ────────────────

type DeserFn = Box<dyn Fn(&[u8]) -> AnyItem + Send + Sync>;

static TYPE_REGISTRY: LazyLock<RwLock<std::collections::HashMap<u64, DeserFn>>> =
    LazyLock::new(|| RwLock::new(std::collections::HashMap::new()));

pub(crate) fn register_type<T>()
where
    T: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    let type_hash = seahash::hash(std::any::type_name::<T>().as_bytes());
    let needs_insert = {
        let reg = TYPE_REGISTRY.read().unwrap();
        !reg.contains_key(&type_hash)
    };
    if needs_insert {
        let mut reg = TYPE_REGISTRY.write().unwrap();
        reg.entry(type_hash).or_insert_with(|| {
            Box::new(|bytes: &[u8]| {
                let value: T = bincode::deserialize(bytes).expect("AnyItem deserialization failed");
                AnyItem(Box::new(value))
            })
        });
    }
}

// ── AnyItem ─────────────────────────────────────────────────────────

/// Cloneable, type-erased wrapper for pipeline elements.
///
/// All values flowing through the Timely dataflow are wrapped in `AnyItem`.
/// With `Clone` bounds on `StreamFunction::Input`/`Output`, the clone is
/// genuine (not a panic stub), enabling future use of Timely's `Exchange` pact.
pub(crate) struct AnyItem(Box<dyn CloneAnySend>);

impl std::fmt::Debug for AnyItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AnyItem")
            .field(&self.0.debug_repr())
            .finish()
    }
}

impl Clone for AnyItem {
    fn clone(&self) -> Self {
        AnyItem(self.0.clone_box())
    }
}

impl AnyItem {
    /// Wrap a concrete typed value.
    pub(crate) fn new<T>(value: T) -> Self
    where
        T: Clone
            + Send
            + std::fmt::Debug
            + serde::Serialize
            + serde::de::DeserializeOwned
            + 'static,
    {
        register_type::<T>();
        AnyItem(Box::new(value))
    }

    /// Consume and downcast to concrete type `T`. Panics on type mismatch.
    pub(crate) fn downcast<T: 'static>(self) -> T {
        *self
            .0
            .into_any()
            .downcast::<T>()
            .unwrap_or_else(|_| panic!("AnyItem: expected {}", std::any::type_name::<T>()))
    }

    /// Borrow and downcast to `&T`. Panics on type mismatch.
    pub(crate) fn downcast_ref<T: 'static>(&self) -> &T {
        self.0
            .as_any_ref()
            .downcast_ref::<T>()
            .unwrap_or_else(|| panic!("AnyItem: expected &{}", std::any::type_name::<T>()))
    }

    /// Best-effort debug representation for DLQ diagnostics.
    #[allow(dead_code)]
    pub(crate) fn debug_repr(&self) -> String {
        self.0.debug_repr()
    }
}

impl serde::Serialize for AnyItem {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeTuple;
        let mut tup = serializer.serialize_tuple(2)?;
        tup.serialize_element(&self.0.stable_type_id())?;
        tup.serialize_element(&self.0.serialize_bytes())?;
        tup.end()
    }
}

impl<'de> serde::Deserialize<'de> for AnyItem {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (type_hash, bytes): (u64, Vec<u8>) = serde::Deserialize::deserialize(deserializer)?;
        let reg = TYPE_REGISTRY.read().unwrap();
        let deser_fn = reg.get(&type_hash).ok_or_else(|| {
            serde::de::Error::custom(format!("unknown AnyItem type hash: {type_hash}"))
        })?;
        Ok(deser_fn(&bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_output_type_makes_anyitem_deserializable() {
        register_type::<String>();

        let item = AnyItem::new("hello".to_string());
        let bytes = bincode::serialize(&item).unwrap();

        let restored: AnyItem = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.downcast::<String>(), "hello");
    }

    #[test]
    fn register_type_is_idempotent() {
        register_type::<u32>();
        register_type::<u32>();

        let item = AnyItem::new(42u32);
        let bytes = bincode::serialize(&item).unwrap();
        let restored: AnyItem = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.downcast::<u32>(), 42);
    }
}
