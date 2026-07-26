//! Build script: locate UCX headers and generate UCP bindings.
//!
//! UCX is Linux-only; this crate is never built for the Windows dev machine.
//! Header search order:
//!   1. `UCX_HOME` (prefix of a custom UCX install, e.g. built from source)
//!   2. pkg-config (`ucx`)
//!   3. default system include paths (rdma-core / MLNX_OFED installs)

use std::env;
use std::path::PathBuf;

fn main() {
    let mut clang_args: Vec<String> = Vec::new();

    if let Ok(home) = env::var("UCX_HOME") {
        clang_args.push(format!("-I{}/include", home));
        println!("cargo:rustc-link-search=native={}/lib", home);
        println!("cargo:rustc-link-search=native={}/lib64", home);
    } else if let Ok(lib) = pkg_config::Config::new().probe("ucx") {
        for path in lib.include_paths {
            clang_args.push(format!("-I{}", path.display()));
        }
    }

    // Link the UCP umbrella library; uct/ucs are pulled in as its deps but are
    // linked explicitly because some distros ship them as separate DSOs.
    println!("cargo:rustc-link-lib=dylib=ucp");
    println!("cargo:rustc-link-lib=dylib=ucs");

    let wrapper = "wrapper.h";
    println!("cargo:rerun-if-changed={}", wrapper);
    println!("cargo:rerun-if-env-changed=UCX_HOME");

    let bindings = bindgen::Builder::default()
        .header(wrapper)
        .clang_args(&clang_args)
        .allowlist_function("ucp_.*")
        .allowlist_type("ucp_.*")
        .allowlist_var("UCP_.*")
        .allowlist_type("ucs_.*")
        .allowlist_var("UCS_.*")
        .allowlist_function("ucs_.*")
        .allowlist_type("sockaddr.*")
        .allowlist_type("sa_family.*")
        .default_enum_style(bindgen::EnumVariation::Rust {
            non_exhaustive: true,
        })
        .derive_debug(true)
        .generate_comments(false)
        .generate()
        .expect("failed to generate UCX bindings; is ucp.h available? (set UCX_HOME)");

    let out = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out.join("bindings.rs"))
        .expect("failed to write bindings");
}
