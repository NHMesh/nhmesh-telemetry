use crate::util::spawn_app;
use reqwest::StatusCode;
use serde_json::json;

#[tokio::test]
async fn test_create_user() {
    let app_url = spawn_app().await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/user/create", app_url))
        .json(&json!({
            "username": "nhmesh",
            "password": "hunter2",
            "is_superuser": true
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["username"], "nhmesh");
}