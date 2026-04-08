//! Cloneable, type-erased wrapper for pipeline elements.
//!
//! All values flowing through the Timely dataflow are wrapped in [`AnyItem`].
//! This module also contains the global type registry for cross-process
//! serialization via bincode.

use std::any::Any;
use std::fmt;
use std::sync::{LazyLock, RwLock};

// ── DowncastError ──────────────────────────────────────────────────

/// Error returned when an [`AnyItem`] downcast fails due to a type mismatch.
///
/// Contains the expected type name for diagnostic purposes. The original
/// `AnyItem` is consumed and cannot be recovered (use `try_downcast_ref`
/// for a non-consuming alternative).
#[derive(Debug, Clone)]
pub(crate) struct DowncastError {
    /// The expected concrete type name (from `std::any::type_name::<T>()`).
    pub expected: &'static str,
}

impl fmt::Display for DowncastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AnyItem downcast failed: expected {}", self.expected)
    }
}

impl std::error::Error for DowncastError {}

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
    fn serialize_bytes(&self) -> Result<Vec<u8>, bincode::Error>;
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

    fn serialize_bytes(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }
}

// ── Global type registry for AnyItem deserialization ────────────────

type DeserFn = Box<dyn Fn(&[u8]) -> Result<AnyItem, bincode::Error> + Send + Sync>;

static TYPE_REGISTRY: LazyLock<RwLock<std::collections::HashMap<u64, DeserFn>>> =
    LazyLock::new(|| RwLock::new(std::collections::HashMap::new()));

pub(crate) fn register_type<T>()
where
    T: Clone + Send + std::fmt::Debug + serde::Serialize + serde::de::DeserializeOwned + 'static,
{
    let type_hash = seahash::hash(std::any::type_name::<T>().as_bytes());

    // Fast path: check with a read lock (no contention). This avoids taking
    // the write lock on every AnyItem::new() call after the type is registered.
    {
        let reg = TYPE_REGISTRY.read().unwrap_or_else(|e| {
            tracing::warn!("TYPE_REGISTRY read lock poisoned, recovering: {e}");
            e.into_inner()
        });
        if reg.contains_key(&type_hash) {
            return;
        }
    }

    // Slow path: take a single write lock and use or_insert_with to
    // atomically check-and-insert, eliminating any TOCTOU race between
    // the read check above and this write.
    let mut reg = TYPE_REGISTRY.write().unwrap_or_else(|e| {
        tracing::warn!("TYPE_REGISTRY write lock poisoned, recovering: {e}");
        e.into_inner()
    });
    reg.entry(type_hash).or_insert_with(|| {
        Box::new(|bytes: &[u8]| {
            let value: T = bincode::deserialize(bytes)?;
            Ok(AnyItem(Box::new(value)))
        })
    });
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

    /// Consume and downcast to concrete type `T`.
    ///
    /// Returns `Err(DowncastError)` if the contained value is not of type `T`.
    pub(crate) fn try_downcast<T: 'static>(self) -> Result<T, DowncastError> {
        self.0
            .into_any()
            .downcast::<T>()
            .map(|b| *b)
            .map_err(|_| DowncastError {
                expected: std::any::type_name::<T>(),
            })
    }

    /// Borrow and downcast to `&T`.
    ///
    /// Returns `Err(DowncastError)` if the contained value is not of type `T`.
    #[allow(dead_code)]
    pub(crate) fn try_downcast_ref<T: 'static>(&self) -> Result<&T, DowncastError> {
        self.0
            .as_any_ref()
            .downcast_ref::<T>()
            .ok_or(DowncastError {
                expected: std::any::type_name::<T>(),
            })
    }

    /// Consume and downcast to concrete type `T`. Panics on type mismatch.
    #[deprecated(note = "use try_downcast() which returns Result instead of panicking")]
    #[allow(dead_code)]
    pub(crate) fn downcast<T: 'static>(self) -> T {
        *self
            .0
            .into_any()
            .downcast::<T>()
            .unwrap_or_else(|_| panic!("AnyItem: expected {}", std::any::type_name::<T>()))
    }

    /// Borrow and downcast to `&T`. Panics on type mismatch.
    #[deprecated(note = "use try_downcast_ref() which returns Result instead of panicking")]
    #[allow(dead_code)]
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
        let bytes = self
            .0
            .serialize_bytes()
            .map_err(serde::ser::Error::custom)?;
        let mut tup = serializer.serialize_tuple(2)?;
        tup.serialize_element(&self.0.stable_type_id())?;
        tup.serialize_element(&bytes)?;
        tup.end()
    }
}

impl<'de> serde::Deserialize<'de> for AnyItem {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let (type_hash, bytes): (u64, Vec<u8>) = serde::Deserialize::deserialize(deserializer)?;
        let reg = TYPE_REGISTRY.read().unwrap_or_else(|e| {
            tracing::warn!("TYPE_REGISTRY read lock poisoned during deserialize, recovering: {e}");
            e.into_inner()
        });
        let deser_fn = reg.get(&type_hash).ok_or_else(|| {
            serde::de::Error::custom(format!("unknown AnyItem type hash: {type_hash}"))
        })?;
        deser_fn(&bytes).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn register_output_type_makes_anyitem_deserializable() {
        register_type::<String>();

        let item = AnyItem::new("hello".to_string());
        let bytes = bincode::serialize(&item).unwrap();

        let restored: AnyItem = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.try_downcast::<String>().unwrap(), "hello");
    }

    #[test]
    fn register_type_is_idempotent() {
        register_type::<u32>();
        register_type::<u32>();

        let item = AnyItem::new(42u32);
        let bytes = bincode::serialize(&item).unwrap();
        let restored: AnyItem = bincode::deserialize(&bytes).unwrap();
        assert_eq!(restored.try_downcast::<u32>().unwrap(), 42);
    }

    #[test]
    fn try_downcast_success() {
        let item = AnyItem::new(42i32);
        assert_eq!(item.try_downcast::<i32>().unwrap(), 42);
    }

    #[test]
    fn try_downcast_wrong_type_returns_error() {
        let item = AnyItem::new(42i32);
        let err = item.try_downcast::<String>().unwrap_err();
        assert!(
            err.to_string().contains("String"),
            "error should mention expected type, got: {err}"
        );
    }

    #[test]
    fn try_downcast_ref_success() {
        let item = AnyItem::new("hello".to_string());
        let val = item.try_downcast_ref::<String>().unwrap();
        assert_eq!(val, "hello");
    }

    #[test]
    fn try_downcast_ref_wrong_type_returns_error() {
        let item = AnyItem::new(42i32);
        let err = item.try_downcast_ref::<String>().unwrap_err();
        assert!(
            err.to_string().contains("String"),
            "error should mention expected type, got: {err}"
        );
    }
}
