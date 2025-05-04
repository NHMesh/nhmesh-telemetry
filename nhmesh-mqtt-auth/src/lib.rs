use actix_web::dev::Server;
use actix_web::{App, HttpServer};
use std::net::TcpListener;

mod config;
mod models;
mod routes;

pub use config::*;

pub fn run(listener: TcpListener) -> Result<Server, std::io::Error> {
    let server = HttpServer::new(|| App::new().service(routes::health))
        .listen(listener)?
        .run();

    Ok(server)
}
