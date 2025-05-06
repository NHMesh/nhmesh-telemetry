use actix_web::dev::Server;
use actix_web::{App, HttpServer, web};
use std::net::TcpListener;
use entity::sea_orm::DatabaseConnection;

mod config;
mod models;
mod routes;

pub use config::*;

#[derive(Clone, Debug)]
pub(crate)struct AppState {
    db: DatabaseConnection,
}

pub fn run(listener: TcpListener, db: DatabaseConnection) -> Result<Server, std::io::Error> {
    let server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState { db: db.clone() }))
            .service(routes::health)
            .service(routes::mqtt_auth)
            .service(routes::create_new_user)
            .service(routes::get_user)
    })
    .listen(listener)?
    .run();

    Ok(server)
}
