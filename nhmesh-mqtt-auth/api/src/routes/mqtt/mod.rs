use actix_web::{HttpResponse, post, web::Json};
use tracing::instrument;

use crate::models::MqttAuth;

#[post("/mqtt/auth")]
#[instrument(level = "info")]
pub async fn mqtt_auth(body: Json<MqttAuth>) -> HttpResponse {
    HttpResponse::Ok().body("allow")
}
