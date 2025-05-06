use crate::util::spawn_app;
use crate::user::create_user;
use reqwest::StatusCode;
use serde_json::json;

#[tokio::test]
async fn test_mqtt_auth() {
    let app_url = spawn_app().await;
    let client = reqwest::Client::new();
    let username = "nhmesh";
    let password = "hunter2";
    create_user(&app_url, &client, username, password, true).await.unwrap();
    let response = client
        .post(format!("{}/mqtt/auth", app_url))
        .json(&json!({
            "clientid": "test",
            "username": username,
            "password": password,
            "ipaddr": "127.0.0.1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "allow");
}


#[tokio::test]
async fn test_mqtt_auth_invalid_password() {
    let app_url = spawn_app().await;
    let client = reqwest::Client::new();
    let username = "nhmesh";
    let password = "hunter2";
    let invalid_password = "hunter3";
    create_user(&app_url, &client, username, password, true).await.unwrap();
    let response = client
        .post(format!("{}/mqtt/auth", app_url))
        .json(&json!({
            "clientid": "test",
            "username": username,
            "password": invalid_password,
            "ipaddr": "127.0.0.1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "deny");
}