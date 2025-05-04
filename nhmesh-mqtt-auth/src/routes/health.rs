use actix_web::{HttpResponse, get};
use serde_json::json;
use tracing::instrument;

#[get("/health")]
#[instrument(level = "debug")]
pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(json!({
        "status": "ok"
    }))
}
