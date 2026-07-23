use std::env;
use std::path::PathBuf;
use std::process::Command;

fn run(mut cmd: Command) {
    let status = cmd.status().expect("failed to spawn C build command");
    if !status.success() {
        panic!("C build command failed with status {status}");
    }
}

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").unwrap());
    let include_dir = manifest_dir.join("c/include");
    let src = manifest_dir.join("c/src/varint.c");
    let obj = out_dir.join("varint.o");
    let lib = out_dir.join("libnormfs_uintn_c.a");

    println!("cargo:rerun-if-changed={}", src.display());
    println!(
        "cargo:rerun-if-changed={}",
        include_dir.join("uintn/varint.h").display()
    );

    let cc = env::var_os("CC").unwrap_or_else(|| "cc".into());
    let ar = env::var_os("AR").unwrap_or_else(|| "ar".into());

    let mut cc_cmd = Command::new(cc);
    cc_cmd
        .arg("-std=c99")
        .arg("-O3")
        .arg("-Wall")
        .arg("-Wextra")
        .arg("-Werror")
        .arg("-pedantic")
        .arg("-I")
        .arg(&include_dir)
        .arg("-c")
        .arg(&src)
        .arg("-o")
        .arg(&obj);
    run(cc_cmd);

    let mut ar_cmd = Command::new(ar);
    ar_cmd.arg("crs").arg(&lib).arg(&obj);
    run(ar_cmd);

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=normfs_uintn_c");
}
