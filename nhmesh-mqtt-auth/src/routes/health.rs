use actix_web::HttpResponse;
use serde_json::json;


pub async fn health() -> HttpResponse {
    HttpResponse::Ok().json(json!({
        "status": "ok"
    }))
}
