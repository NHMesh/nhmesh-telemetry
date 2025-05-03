use nhmesh_mqtt_auth::run;
use std::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let listener = TcpListener::bind("127.0.0.1:8080")?;
    run(listener)?.await?;
    Ok(())
}
