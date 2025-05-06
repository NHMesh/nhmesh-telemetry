use config::Config;
use serde::Deserialize;

#[derive(Debug, Deserialize, Default)]
pub struct Settings {
    pub app: AppConfig,
    pub database: DatabaseConfig,
}

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub port: u16,
    pub host: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            port: 8080,
            host: "127.0.0.1".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "sqlite://auth-db.sqlite?mode=rwc".to_string(),
        }
    }
}

pub fn get_config() -> Result<Settings, config::ConfigError> {
    let config = Config::builder()
        .add_source(config::File::with_name("config.yaml").format(config::FileFormat::Yaml))
        .build()?;

    config.try_deserialize::<Settings>()
}
