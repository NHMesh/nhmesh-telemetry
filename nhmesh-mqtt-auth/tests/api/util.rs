use std::net::TcpListener;
use nhmesh_mqtt_auth::run;

pub fn spawn_app() -> String {
    // Bind to random port
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to random port");
    // Get port
    let port = listener.local_addr().unwrap().port();
    // Run server
    let server = run(listener).expect("Failed to bind address");
    let _ = tokio::spawn(server);
    // Return test url 
    format!("http://127.0.0.1:{}", port)
}