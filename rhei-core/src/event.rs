use serde::{Deserialize, Serialize};

/// Marker trait for events that can flow through the stream processing engine.
///
/// All user-facing event types must implement this trait.
pub trait Event: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static {}

// Blanket implementation: any type satisfying the bounds is automatically an Event.
impl<T> Event for T where T: Serialize + for<'de> Deserialize<'de> + Send + Sync + Clone + 'static {}

/// Serialize an event to JSON bytes.
pub fn to_json<T: Event>(event: &T) -> anyhow::Result<Vec<u8>> {
    Ok(serde_json::to_vec(event)?)
}

/// Deserialize an event from JSON bytes.
pub fn from_json<T: Event>(bytes: &[u8]) -> anyhow::Result<T> {
    Ok(serde_json::from_slice(bytes)?)
}

/// Serialize an event to bincode bytes.
pub fn to_bincode<T: Event>(event: &T) -> anyhow::Result<Vec<u8>> {
    Ok(bincode::serialize(event)?)
}

/// Deserialize an event from bincode bytes.
pub fn from_bincode<T: Event>(bytes: &[u8]) -> anyhow::Result<T> {
    Ok(bincode::deserialize(bytes)?)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct WordCount {
        word: String,
        count: u64,
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct Metric {
        name: String,
        value: f64,
        tags: Vec<String>,
    }

    fn assert_event<T: Event>() {}

    #[test]
    fn word_count_is_event() {
        assert_event::<WordCount>();
    }

    #[test]
    fn metric_is_event() {
        assert_event::<Metric>();
    }

    #[test]
    fn primitives_are_events() {
        assert_event::<String>();
        assert_event::<u64>();
        assert_event::<Vec<u8>>();
    }

    #[test]
    fn json_roundtrip() {
        let wc = WordCount {
            word: "hello".into(),
            count: 42,
        };
        let bytes = to_json(&wc).unwrap();
        let restored: WordCount = from_json(&bytes).unwrap();
        assert_eq!(wc, restored);
    }

    #[test]
    fn bincode_roundtrip() {
        let wc = WordCount {
            word: "rhei".into(),
            count: 7,
        };
        let bytes = to_bincode(&wc).unwrap();
        let restored: WordCount = from_bincode(&bytes).unwrap();
        assert_eq!(wc, restored);
    }

    #[test]
    fn json_roundtrip_complex() {
        let m = Metric {
            name: "cpu_usage".into(),
            value: 0.95,
            tags: vec!["host:web1".into(), "region:us-east".into()],
        };
        let bytes = to_json(&m).unwrap();
        let restored: Metric = from_json(&bytes).unwrap();
        assert_eq!(m, restored);
    }

    #[test]
    fn bincode_roundtrip_complex() {
        let m = Metric {
            name: "mem_free".into(),
            value: 1024.5,
            tags: vec!["dc:aws".into()],
        };
        let bytes = to_bincode(&m).unwrap();
        let restored: Metric = from_bincode(&bytes).unwrap();
        assert_eq!(m, restored);
    }
}
