use crate::util::spawn_app;
use reqwest::StatusCode;
use serde_json::json;

pub async fn create_user(app_url: &str, client: &reqwest::Client, username: &str, password: &str, is_superuser: bool) -> Result<reqwest::Response, reqwest::Error> {
    client
        .post(format!("{}/user/create", app_url))
        .json(&json!({
            "username": username,
            "password": password,
            "is_superuser": is_superuser
        }))
        .send()
        .await
}

pub async fn get_user(app_url: &str, client: &reqwest::Client, username: &str) -> Result<reqwest::Response, reqwest::Error> {
    client
        .get(format!("{}/user/{}", app_url, username))
        .send()
        .await
}

#[tokio::test]
async fn test_create_user() {
    let app_url = spawn_app().await;
    let client = reqwest::Client::new();
    let response = create_user(&app_url, &client, "nhmesh", "hunter2", true).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["username"], "nhmesh");
}

#[tokio::test]
async fn test_get_user() {
    let app_url = spawn_app().await;
    let user = "nhmesh";
    let client = reqwest::Client::new();
    let create_response = create_user(&app_url, &client, user, "hunter2", true).await.unwrap();
    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let user_id = create_body["id"].as_str().unwrap();
    let response = get_user(&app_url, &client, user).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["username"], user);
    assert_eq!(body["is_superuser"], true);
    assert_eq!(body["id"], user_id);
}