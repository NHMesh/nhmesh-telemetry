use actix_web::{web, App, HttpServer};
use actix_web::dev::Server;

mod routes;

pub fn run() -> Result<Server, std::io::Error> {
    let server = HttpServer::new(|| {
        App::new()
            .route("/health", web::get().to(routes::health))
    })
    .bind("127.0.0.1:8080")?
    .run();

    Ok(server)
}