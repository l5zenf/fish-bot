use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// Dependency container — holds Arc<dyn Any + Send + Sync> keyed by TypeId.
///
/// # Usage
///
/// ```ignore
/// // Register
/// let ctx = Ctx::new();
/// ctx.insert(my_db_pool);
///
/// // Retrieve
/// let pool: Arc<PgPool> = ctx.get::<PgPool>().or_else(|| { /* handle missing dep */ });
/// ```
pub struct Ctx {
    inner: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl Ctx {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Insert a value into the container. Overwrites any previous value of the same type.
    pub fn insert<T: Any + Send + Sync>(&self, value: T) {
        let mut map = self.inner.write();
        map.insert(TypeId::of::<T>(), Arc::new(value));
    }

    /// Insert an already-Arc-wrapped value.
    pub fn insert_arc<T: Any + Send + Sync>(&self, value: Arc<T>) {
        let mut map = self.inner.write();
        map.insert(TypeId::of::<T>(), value as Arc<dyn Any + Send + Sync>);
    }

    /// Retrieve a value by type. Returns None if the type is not registered.
    pub fn get<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        let map = self.inner.read();
        map.get(&TypeId::of::<T>())
            .and_then(|arc: &Arc<dyn Any + Send + Sync>| arc.clone().downcast::<T>().ok())
    }

    /// Remove a value by type.
    pub fn remove<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        let mut map = self.inner.write();
        map.remove(&TypeId::of::<T>())
            .and_then(|arc: Arc<dyn Any + Send + Sync>| arc.downcast::<T>().ok())
    }
}

impl Default for Ctx {
    fn default() -> Self {
        Self::new()
    }
}
