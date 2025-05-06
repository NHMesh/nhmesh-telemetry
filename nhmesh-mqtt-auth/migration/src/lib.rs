pub use sea_orm_migration::prelude::*;

mod m20220101_000001_create_users_table;
mod m20250504_231730_create_superuser;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(m20220101_000001_create_users_table::Migration),
            Box::new(m20250504_231730_create_superuser::Migration),
        ]
    }
}
