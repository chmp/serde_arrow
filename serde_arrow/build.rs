fn main() {
    #[cfg(feature = "arrow2-0-17")]
    {
        println!("cargo:rustc-cfg=has_arrow2");
        println!("cargo:rustc-cfg=has_arrow2_0_17");
    }

    #[cfg(all(feature = "arrow2-0-16", not(feature = "arrow2-0-17")))]
    {
        println!("cargo:rustc-cfg=has_arrow2");
        println!("cargo:rustc-cfg=has_arrow2_0_16");
    }

    #[cfg(feature = "arrow-39")]
    {
        println!("cargo:rustc-cfg=has_arrow");
        println!("cargo:rustc-cfg=has_arrow_39");
    }

    #[cfg(all(feature = "arrow-38", not(feature = "arrow-39")))]
    {
        println!("cargo:rustc-cfg=has_arrow");
        println!("cargo:rustc-cfg=has_arrow_38");
    }

    #[cfg(all(
        feature = "arrow-37",
        not(feature = "arrow-38"),
        not(feature = "arrow-39"),
    ))]
    {
        println!("cargo:rustc-cfg=has_arrow");
        println!("cargo:rustc-cfg=has_arrow_37");
    }

    #[cfg(all(
        feature = "arrow-36",
        not(feature = "arrow-37"),
        not(feature = "arrow-38"),
        not(feature = "arrow-39"),
    ))]
    {
        println!("cargo:rustc-cfg=has_arrow");
        println!("cargo:rustc-cfg=has_arrow_36");
    }
}
