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
        // arrow-version:insert: #[cfg(feature = "arrow-{version}")]{\n}{version},
        #[cfg(feature = "arrow-52")]
        52,
        #[cfg(feature = "arrow-51")]
        51,
        #[cfg(feature = "arrow-50")]
        50,
        #[cfg(feature = "arrow-49")]
        49,
        #[cfg(feature = "arrow-48")]
        48,
        #[cfg(feature = "arrow-47")]
        47,
        #[cfg(feature = "arrow-46")]
        46,
        #[cfg(feature = "arrow-45")]
        45,
        #[cfg(feature = "arrow-44")]
        44,
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
    ]
    .into_iter()
    .max();

    if let Some(version) = max_arrow_version {
        println!("cargo:rustc-cfg=has_arrow");
        println!("cargo:rustc-cfg=has_arrow_{version}");

        if version >= 47 {
            println!("cargo:rustc-cfg=has_arrow_fixed_binary_support");
        }
    }
}
