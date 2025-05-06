use actix_web::{HttpResponse, post, web, get};
use serde_json::json;
use tracing::instrument;
use crate::models::{CreateUser, UserService};
use crate::AppState;
use anyhow::Context;

#[post("/user/create")]
#[instrument(level = "info")]
pub async fn create_new_user(
    state: web::Data<AppState>,
    user: web::Json<CreateUser>,
) -> HttpResponse {
    let user_service = UserService::new(&state.db);
    match user_service.create_user(user.into_inner()).await {
        Ok(user) => HttpResponse::Ok().json(user),
        Err(e) => {
            tracing::error!("Failed to create user: {}", e);
            HttpResponse::InternalServerError().json(json!({ "error": e.to_string() }))
        }
    }
}

#[get("/user/{username}")]
#[instrument(level = "info")]
pub async fn get_user(
    state: web::Data<AppState>,
    username: web::Path<String>,
) -> HttpResponse {
    let user_service = UserService::new(&state.db);
    match user_service.find_by_username(&username).await {
        Ok(Some(user)) => HttpResponse::Ok().json(user),
        Ok(None) => HttpResponse::NotFound().json(json!({ "error": "User not found" })),
        Err(e) => {
            tracing::error!("Failed to get user: {}", e);
            HttpResponse::InternalServerError().json(json!({ "error": e.to_string() }))
        }
    }
}