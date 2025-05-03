use nhmesh_mqtt_auth::run;


#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    run()?.await?;
    Ok(())
}
