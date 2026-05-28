fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile_protos_with_config(
        prost_build::Config::new(),
        &["proto/dtdb.proto"],
        &["proto"],
    )?;
    Ok(())
}
