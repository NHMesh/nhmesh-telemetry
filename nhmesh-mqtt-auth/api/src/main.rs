use nhmesh_mqtt_auth::{get_config, run};
use std::net::TcpListener;
use tracing_log::LogTracer;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};
use entity::sea_orm::Database;
use migration::{Migrator, MigratorTrait};
use anyhow::{Result, Context};

#[tokio::main]
async fn main() -> Result<()> {
    LogTracer::init().expect("Failed to set logger");
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = Registry::default().with(env_filter).with(fmt::layer());
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

    let config = get_config().expect("Failed to read config.");
    let listener = TcpListener::bind(format!("{}:{}", config.app.host, config.app.port)).context("Failed to bind to address")?;
    let db = Database::connect(config.database.url).await.context("Failed to connect to database")?;
    Migrator::up(&db, None).await.context("Failed to migrate database")?;

    run(listener, db)?.await?;
    Ok(())
}
