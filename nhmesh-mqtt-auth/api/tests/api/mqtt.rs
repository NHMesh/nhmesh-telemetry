use crate::util::spawn_app;
use reqwest::StatusCode;
use serde_json::json;

#[tokio::test]
async fn test_mqtt_auth() {
    let app_url = spawn_app().await;
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/mqtt/auth", app_url))
        .json(&json!({
            "clientid": "test",
            "username": "test",
            "password": "test",
            "ipaddr": "127.0.0.1"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "allow");
}
