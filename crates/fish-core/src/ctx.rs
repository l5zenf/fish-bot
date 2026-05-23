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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq)]
    struct DbPool(u32);

    #[derive(Debug, Clone, PartialEq)]
    struct Config(String);

    #[test]
    fn t1_28_insert_get_roundtrip() -> anyhow::Result<()> {
        let ctx = Ctx::new();
        ctx.insert(DbPool(42));

        let pool = ctx.get::<DbPool>().ok_or_else(|| anyhow::anyhow!("DbPool should exist"))?;
        assert_eq!(pool.0, 42);
        Ok(())
    }

    #[test]
    fn t1_29_get_unregistered_returns_none() {
        let ctx = Ctx::new();
        let pool: Option<Arc<DbPool>> = ctx.get::<DbPool>();
        assert!(pool.is_none());
    }

    #[test]
    fn t1_30_insert_overwrites_same_type() -> anyhow::Result<()> {
        let ctx = Ctx::new();
        ctx.insert(DbPool(1));
        ctx.insert(DbPool(2));

        let pool = ctx.get::<DbPool>().ok_or_else(|| anyhow::anyhow!("DbPool should exist"))?;
        assert_eq!(pool.0, 2);
        Ok(())
    }

    #[test]
    fn t1_31_insert_arc() -> anyhow::Result<()> {
        let ctx = Ctx::new();
        let val = Arc::new(DbPool(99));
        ctx.insert_arc(Arc::clone(&val));
        let pool = ctx.get::<DbPool>().ok_or_else(|| anyhow::anyhow!("DbPool should exist"))?;
        assert_eq!(pool.0, 99);
        Ok(())
    }

    #[test]
    fn t1_32_remove_then_get_returns_none() -> anyhow::Result<()> {
        let ctx = Ctx::new();
        ctx.insert(DbPool(42));
        let removed = ctx.remove::<DbPool>().ok_or_else(|| anyhow::anyhow!("remove should return value"))?;
        assert_eq!(removed.0, 42);

        let pool: Option<Arc<DbPool>> = ctx.get::<DbPool>();
        assert!(pool.is_none());
        Ok(())
    }

    #[test]
    fn t1_33_mixed_types() -> anyhow::Result<()> {
        let ctx = Ctx::new();
        ctx.insert(DbPool(1));
        ctx.insert(Config("prod".into()));

        assert_eq!(ctx.get::<DbPool>().ok_or_else(|| anyhow::anyhow!("DbPool"))?.0, 1);
        assert_eq!(ctx.get::<Config>().ok_or_else(|| anyhow::anyhow!("Config"))?.0, "prod");
        Ok(())
    }
}
