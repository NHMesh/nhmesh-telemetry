use actix_web::{HttpResponse, post, web};
use tracing::instrument;
use crate::AppState;
use secrecy::ExposeSecret;
use crate::models::{MqttAuth, UserService};

#[post("/mqtt/auth")]
#[instrument(level = "info")]
pub async fn mqtt_auth(state: web::Data<AppState>, body: web::Json<MqttAuth>) -> HttpResponse {
    let user_service = UserService::new(&state.db);
    let auth_req = body.into_inner();
    match user_service.verify_user(&auth_req.username, &auth_req.password.expose_secret())
        .await
        .map_err(|e| {
            tracing::warn!("Failed to verify user: {}", e);
            HttpResponse::Unauthorized().body(e.to_string())
        }) {
            Ok(user) => {
                HttpResponse::Ok().body("allow")
            }
            Err(e) => {
                HttpResponse::Ok().body("deny")
            }
        }
}
