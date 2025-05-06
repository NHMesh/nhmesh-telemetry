use crate::util::spawn_app;
use reqwest::StatusCode;

#[tokio::test]
async fn test_health() {
    let app_url = spawn_app().await;
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/health", app_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
