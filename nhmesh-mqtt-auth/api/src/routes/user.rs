use actix_web::{HttpResponse, post, web};
use serde_json::json;
use tracing::instrument;
use crate::models::{CreateUser, VerifyUser};
use crate::AppState;
use anyhow::Context;
use entity::sea_orm::ActiveModelTrait;
use futures::TryFutureExt;
use tracing::debug;
#[post("/user/create")]
#[instrument(level = "info")]
pub async fn create_new_user(
    state: web::Data<AppState>,
    user: web::Json<CreateUser>,
) -> HttpResponse {
    let user = user.into_inner();
    let user_result = async move {
        user.to_model().context("Failed to convert user to model")
    }.and_then(|user| async move { user.insert(&state.db).await.inspect_err(|e| debug!("Failed to insert user: {}", e)).context("Failed to insert user") }).await;
    match user_result {
        Ok(user) => HttpResponse::Ok().json(json!({ "id": user.id, "username": user.username })),
        Err(e) => {
            tracing::error!("Failed to create user: {}", e);
            HttpResponse::InternalServerError().json(json!({ "error": e.to_string() }))
        }
    }
}