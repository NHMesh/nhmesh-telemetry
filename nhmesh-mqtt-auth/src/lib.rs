use actix_web::{web, App, HttpServer};
use actix_web::dev::Server;
use std::net::TcpListener;

mod routes;

pub fn run(listener: TcpListener) -> Result<Server, std::io::Error> {
    let server = HttpServer::new(|| {
        App::new()
            .route("/health", web::get().to(routes::health))
    })
    .listen(listener)?
    .run();

    Ok(server)
}