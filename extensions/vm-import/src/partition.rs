#[derive(Hash, Eq, PartialEq, Clone)]
pub struct PartitionKey {
    pub endpoint: String,
}

impl PartitionKey {
    pub fn new(endpoint: String) -> Self {
        Self { endpoint }
    }
}
