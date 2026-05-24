fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc_path = protobuf_src::protoc();
    unsafe {
        std::env::set_var("PROTOC", protoc_path);
    }

    tonic_build::configure().compile_protos_with_config(
        prost_build::Config::new(),
        &["proto/dtdb.proto"],
        &["proto"],
    )?;
    Ok(())
}
