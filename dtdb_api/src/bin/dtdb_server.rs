use dtdb_api::proto::duct_tape_db_service_server::DuctTapeDbServiceServer;
use dtdb_api::server::DuctTapeDbServiceImpl;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use tonic::transport::Server;

#[tokio::main]
#[allow(clippy::result_large_err)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut data_dir = PathBuf::from("./data");
    let mut bind_addr_str = env::var("DTDB_BIND").unwrap_or_else(|_| "127.0.0.1:50051".to_string());
    let mut auth_token = env::var("DTDB_AUTH_TOKEN").ok();
    let mut tls_cert = env::var("DTDB_TLS_CERT").ok();
    let mut tls_key = env::var("DTDB_TLS_KEY").ok();

    let mut args_iter = env::args().skip(1).peekable();
    while let Some(arg) = args_iter.next() {
        match arg.as_str() {
            "--bind" => {
                if let Some(val) = args_iter.next() {
                    bind_addr_str = val;
                } else {
                    return Err("Missing value for --bind flag".into());
                }
            }
            "--data" => {
                if let Some(val) = args_iter.next() {
                    data_dir = PathBuf::from(val);
                } else {
                    return Err("Missing value for --data flag".into());
                }
            }
            "--auth-token" => {
                if let Some(val) = args_iter.next() {
                    auth_token = Some(val);
                } else {
                    return Err("Missing value for --auth-token flag".into());
                }
            }
            "--tls-cert" => {
                if let Some(val) = args_iter.next() {
                    tls_cert = Some(val);
                } else {
                    return Err("Missing value for --tls-cert flag".into());
                }
            }
            "--tls-key" => {
                if let Some(val) = args_iter.next() {
                    tls_key = Some(val);
                } else {
                    return Err("Missing value for --tls-key flag".into());
                }
            }
            other => {
                if other.starts_with('-') {
                    return Err(format!("Unknown option: {}", other).into());
                }
                // Backward compatibility: first non-flag argument is data directory
                data_dir = PathBuf::from(other);
            }
        }
    }

    println!("Starting DuctTapeDB gRPC Server...");
    println!("Data directory: {:?}", data_dir);

    let has_auth = auth_token.is_some();
    let has_tls = tls_cert.is_some() && tls_key.is_some();
    if let Err(err_msg) = dtdb_api::validate_bind_security(&bind_addr_str, has_auth, has_tls) {
        eprintln!("{}", err_msg);
        eprintln!(
            "Please configure them using --auth-token, --tls-cert, and --tls-key (or corresponding env variables: DTDB_AUTH_TOKEN, DTDB_TLS_CERT, DTDB_TLS_KEY)."
        );
        std::process::exit(1);
    }

    let addr: SocketAddr = bind_addr_str.parse()?;

    let service = DuctTapeDbServiceImpl::new(&data_dir)?;
    println!("Listening on: {}", addr);

    let mut server = Server::builder();

    if let (Some(cert_path), Some(key_path)) = (&tls_cert, &tls_key) {
        println!(
            "Configuring TLS with cert: {}, key: {}",
            cert_path, key_path
        );
        let cert = tokio::fs::read_to_string(cert_path).await?;
        let key = tokio::fs::read_to_string(key_path).await?;
        let identity = tonic::transport::Identity::from_pem(cert, key);
        let tls_config = tonic::transport::ServerTlsConfig::new().identity(identity);
        server = server.tls_config(tls_config)?;
    }

    let auth_token_clone = auth_token.clone();
    let interceptor = move |req: tonic::Request<()>| -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(ref token) = auth_token_clone {
            match req.metadata().get("authorization") {
                Some(header_val) => {
                    let expected_header = format!("Bearer {}", token);
                    if header_val.to_str().unwrap_or("") == expected_header {
                        Ok(req)
                    } else {
                        Err(tonic::Status::unauthenticated("Invalid auth token"))
                    }
                }
                None => Err(tonic::Status::unauthenticated("Missing auth token")),
            }
        } else {
            Ok(req)
        }
    };

    let service_server = DuctTapeDbServiceServer::with_interceptor(service, interceptor);

    server.add_service(service_server).serve(addr).await?;

    Ok(())
}
