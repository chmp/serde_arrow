fn main() {
    let max_arrow_version: Option<usize> = [
        // arrow-version:insert: #[cfg(feature = "arrow-{version}")]{\n}{version},
        #[cfg(feature = "arrow-59")]
        59,
        #[cfg(feature = "arrow-58")]
        58,
        #[cfg(feature = "arrow-57")]
        57,
        #[cfg(feature = "arrow-56")]
        56,
        #[cfg(feature = "arrow-55")]
        55,
        #[cfg(feature = "arrow-54")]
        54,
        #[cfg(feature = "arrow-53")]
        53,
    ]
    .into_iter()
    .max();

    if let Some(version) = max_arrow_version {
        println!("cargo:rustc-cfg=has_arrow");
        println!("cargo:rustc-cfg=has_arrow_{version}");

        println!("cargo:rustc-cfg=has_arrow_fixed_binary_support");
        println!("cargo:rustc-cfg=has_arrow_bytes_view_support");
    }
}
