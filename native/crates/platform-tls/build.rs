use std::process::Command;
fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var_os("CARGO_FEATURE_GNUTLS_PROBE").is_none() {
        return;
    }
    assert_eq!(
        std::env::var("HOST").unwrap(),
        std::env::var("TARGET").unwrap(),
        "qualification requires native target pkg-config and ABI evidence; cross builds are not admitted"
    );
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");
    let output = Command::new("pkg-config")
        .args(["--libs", "gnutls >= 3.6.4"])
        .output()
        .expect("installed GnuTLS is a qualification prerequisite");
    assert!(output.status.success(), "GnuTLS pkg-config unavailable");
    for flag in String::from_utf8(output.stdout).unwrap().split_whitespace() {
        if let Some(path) = flag.strip_prefix("-L") {
            println!("cargo:rustc-link-search=native={path}");
        } else if let Some(name) = flag.strip_prefix("-l") {
            println!("cargo:rustc-link-lib={name}");
        } else {
            panic!("unreviewed native linker flag {flag}");
        }
    }
}
