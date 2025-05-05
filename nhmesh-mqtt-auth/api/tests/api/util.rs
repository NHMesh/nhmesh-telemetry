use nhmesh_mqtt_auth::run;
use std::net::TcpListener;
use entity::sea_orm::Database;
use migration::{Migrator, MigratorTrait};
use tracing::Subscriber;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};
use tracing_log::LogTracer;
use once_cell::sync::Lazy;

static TRACING: Lazy<()> = Lazy::new(|| {
    let subscriber = get_subscriber("debug".to_string());
    init_subscriber(subscriber);
});

pub fn get_subscriber(env_filter: String) -> impl Subscriber + Send + Sync {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(env_filter));
    let subscriber = Registry::default().with(env_filter).with(fmt::layer());
    subscriber
}

pub fn init_subscriber(subscriber: impl Subscriber + Send + Sync) {
    LogTracer::init().expect("Failed to set logger");
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
}




pub async fn spawn_app() -> String {
    // Initialize tracing
    Lazy::force(&TRACING);
    // Bind to random port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
    // Get port
    let port = listener.local_addr().unwrap().port();

    let db = Database::connect("sqlite::memory:").await.expect("Failed to connect to database");
    Migrator::up(&db, None).await.expect("Failed to migrate database");
    // Run server
    let server = run(listener, db).expect("Failed to bind address");
    let _ = tokio::spawn(server);
    // Return test url
    format!("http://127.0.0.1:{}", port)
}
