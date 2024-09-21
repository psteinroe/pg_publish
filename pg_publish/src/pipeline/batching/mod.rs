pub mod stream;

/// A trait to indicate which items in a stream can be the last in a batch.
pub trait BatchBoundary: Sized {
    fn is_last_in_batch(&self) -> bool;
    fn is_batch_relevant(&self) -> bool;
}

// For an item wrapped in a result we fall back to the item
// for the Ok variant and always return true for an Err variant
// to fail fast as this batch is anyway going to fail.
impl<T: BatchBoundary, E> BatchBoundary for Result<T, E> {
    fn is_last_in_batch(&self) -> bool {
        match self {
            Ok(v) => v.is_last_in_batch(),
            Err(_) => true,
        }
    }

    fn is_batch_relevant(&self) -> bool {
        match self {
            Ok(v) => v.is_batch_relevant(),
            Err(_) => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct BatchConfig {
    max_batch_size: usize,
}

impl BatchConfig {
    pub fn new(max_batch_size: usize) -> BatchConfig {
        BatchConfig { max_batch_size }
    }
}
