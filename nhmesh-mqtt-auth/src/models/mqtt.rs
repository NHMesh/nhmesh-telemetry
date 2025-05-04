use serde::Deserialize;
use std::net::IpAddr;

#[derive(Debug, Deserialize)]
pub struct MqttAuth {
    #[serde(rename = "clientid")]
    pub client_id: String,
    pub username: String,
    pub password: String,
    pub ip: IpAddr,
}
