use secrecy::SecretString;
use serde::Deserialize;
use std::net::IpAddr;

#[derive(Debug, Deserialize)]
pub struct MqttAuth {
    #[serde(rename = "clientid")]
    pub client_id: String,
    pub username: String,
    pub password: SecretString,
    #[serde(rename = "ipaddr")]
    pub ip_addr: IpAddr,
}
