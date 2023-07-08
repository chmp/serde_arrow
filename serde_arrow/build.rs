fn main() {
    let max_arrow2_version: Option<usize> = [
        #[cfg(feature = "arrow2-0-17")]
        17,
        #[cfg(feature = "arrow2-0-16")]
        16,
    ]
    .into_iter()
    .max();

    if let Some(version) = max_arrow2_version {
        println!("cargo:rustc-cfg=has_arrow2");
        println!("cargo:rustc-cfg=has_arrow2_0_{version}");
    }

    let max_arrow_version: Option<usize> = [
        #[cfg(feature = "arrow-43")]
        43,
        #[cfg(feature = "arrow-42")]
        42,
        #[cfg(feature = "arrow-41")]
        41,
        #[cfg(feature = "arrow-40")]
        40,
        #[cfg(feature = "arrow-39")]
        39,
        #[cfg(feature = "arrow-38")]
        38,
        #[cfg(feature = "arrow-37")]
        37,
        #[cfg(feature = "arrow-36")]
        36,
    ]
    .into_iter()
    .max();

    if let Some(version) = max_arrow_version {
        println!("cargo:rustc-cfg=has_arrow");
        println!("cargo:rustc-cfg=has_arrow_{version}");
    }
}
