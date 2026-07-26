// Compiles src/rpc/proto/kv.proto into Rust with tonic-build. The generated code goes
// to OUT_DIR and is pulled in via tonic::include_proto! (see src/rpc/mod.rs).
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "src/rpc/proto/kv.proto";

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&[proto], &["src/rpc/proto"])?;

    // Only rebuild when the schema changes.
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
