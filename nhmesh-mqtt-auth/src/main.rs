use nhmesh_mqtt_auth::{get_config, run};
use std::net::TcpListener;
use tracing_log::LogTracer;
use tracing_subscriber::{EnvFilter, Registry, fmt, layer::SubscriberExt};

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    LogTracer::init().expect("Failed to set logger");
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = Registry::default().with(env_filter).with(fmt::layer());
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");

    let config = get_config().expect("Failed to read config.");
    let listener = TcpListener::bind(format!("{}:{}", config.app.host, config.app.port))?;
    run(listener)?.await?;
    Ok(())
}
