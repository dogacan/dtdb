use std::env;
use std::path::PathBuf;
use tonic::transport::Server;
use dtdb_api::proto::duct_tape_db_service_server::DuctTapeDbServiceServer;
use dtdb_api::server::DuctTapeDbServiceImpl;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let data_dir = if args.len() > 1 {
        PathBuf::from(&args[1])
    } else {
        PathBuf::from("./data")
    };

    println!("Starting DuctTapeDB gRPC Server...");
    println!("Data directory: {:?}", data_dir);

    let service = DuctTapeDbServiceImpl::new(&data_dir)?;
    let addr = "127.0.0.1:50051".parse()?;
    println!("Listening on: {}", addr);

    Server::builder()
        .add_service(DuctTapeDbServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
