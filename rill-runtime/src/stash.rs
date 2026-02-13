use std::collections::VecDeque;

/// Stash for events waiting to be processed.
///
/// When an async operator cannot complete processing synchronously (e.g. state
/// cache miss requiring a backend fetch), the event and its capability are
/// stashed here. Events are processed in FIFO order to preserve per-key
/// ordering guarantees.
pub struct Stash<T> {
    queue: VecDeque<(T, Option<u64>)>,
}

impl<T> Stash<T> {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::new(),
        }
    }

    /// Stash an element with its associated capability (timestamp).
    pub fn push(&mut self, element: T, capability: Option<u64>) {
        self.queue.push_back((element, capability));
    }

    /// Pop the next element in FIFO order.
    pub fn pop(&mut self) -> Option<(T, Option<u64>)> {
        self.queue.pop_front()
    }

    /// Peek at the next element without removing it.
    pub fn peek(&self) -> Option<&(T, Option<u64>)> {
        self.queue.front()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }
}

impl<T> Default for Stash<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fifo_ordering() {
        let mut stash = Stash::new();
        stash.push("a", Some(1));
        stash.push("b", Some(2));
        stash.push("c", Some(3));

        assert_eq!(stash.len(), 3);
        assert_eq!(stash.pop(), Some(("a", Some(1))));
        assert_eq!(stash.pop(), Some(("b", Some(2))));
        assert_eq!(stash.pop(), Some(("c", Some(3))));
        assert!(stash.is_empty());
    }

    #[test]
    fn preserves_capabilities() {
        let mut stash = Stash::new();
        stash.push("event", Some(42));

        let (elem, cap) = stash.pop().unwrap();
        assert_eq!(elem, "event");
        assert_eq!(cap, Some(42));
    }

    #[test]
    fn ordering_under_stash_unstash() {
        // Simulate: Event A triggers state fetch (stashed), Event B arrives,
        // both should process in A, B order.
        let mut stash = Stash::new();
        stash.push("A", Some(1)); // A stashed (state miss)
        stash.push("B", Some(2)); // B arrives while A pending

        // When state fetch completes, drain in order
        let a = stash.pop().unwrap();
        let b = stash.pop().unwrap();
        assert_eq!(a.0, "A");
        assert_eq!(b.0, "B");
        assert!(a.1.unwrap() < b.1.unwrap());
    }

    #[test]
    fn no_premature_capability_drop() {
        let mut stash: Stash<String> = Stash::new();
        stash.push("event1".into(), Some(10));
        stash.push("event2".into(), Some(10));

        // Capabilities should be retained while in stash
        assert_eq!(stash.len(), 2);
        assert!(stash.peek().is_some());

        // Only dropped after explicit pop
        let (_, cap) = stash.pop().unwrap();
        assert_eq!(cap, Some(10));
        // Still one in stash
        assert_eq!(stash.len(), 1);
    }
}
