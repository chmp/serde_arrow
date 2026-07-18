import argparse
import pathlib
import tomllib


SELF_PATH = pathlib.Path(__file__).parents[1].resolve()
SERDE_ARROW_COMPONENTS = ("arrow-array", "arrow-schema")
MARROW_COMPONENTS = ("arrow-array", "arrow-schema", "arrow-data", "arrow-buffer")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--arrow-features", required=True, nargs="+")
    check_cargo_toml(parser.parse_args())


def check_cargo_toml(args):
    print(":: check Cargo.toml")
    serde_arrow_config = load_config("serde_arrow")
    marrow_config = load_config("marrow")
    marrow_integration_config = load_config("marrow_integration")

    serde_arrow_marrow_dep = serde_arrow_config["dependencies"].get("marrow")
    if serde_arrow_marrow_dep is None:
        raise ValueError("Missing serde_arrow dependency marrow")

    serde_arrow_marrow_version = serde_arrow_marrow_dep.get("version")
    marrow_version = marrow_config["package"]["version"]
    if serde_arrow_marrow_version != marrow_version:
        raise ValueError(
            "Invalid serde_arrow marrow dependency version. "
            f"Expected: {marrow_version}, found: {serde_arrow_marrow_version}"
        )

    check_feature_list(
        "serde_arrow",
        "docs.rs configuration",
        serde_arrow_config["package"]["metadata"]["docs"]["rs"]["features"],
        (args.arrow_features[0],),
    )
    check_feature_list(
        "marrow",
        "docs.rs configuration",
        marrow_config["package"]["metadata"]["docs"]["rs"]["features"],
        ("serde", args.arrow_features[0]),
    )

    for feature in args.arrow_features:
        check_arrow_feature(
            "serde_arrow",
            serde_arrow_config,
            feature,
            SERDE_ARROW_COMPONENTS,
            extra_features=("marrow/arrow-{version}",),
        )
        check_arrow_feature(
            "marrow",
            marrow_config,
            feature,
            MARROW_COMPONENTS,
        )

        *_, version = feature.partition("-")
        check_feature_list(
            "marrow_integration",
            f"feature definition for {feature}",
            marrow_integration_config["features"][feature],
            [
                f"marrow/arrow-{version}",
                f"dep:arrow-array-{version}",
                f"dep:arrow-schema-{version}",
            ],
        )

    for crate, config in [
        ("serde_arrow", serde_arrow_config),
        ("marrow", marrow_config),
    ]:
        for name, dep in config["dependencies"].items():
            if dep.get("default-features", True):
                raise ValueError(
                    f"Default features for {crate} dependency {name} not deactivated"
                )


def load_config(crate):
    with open(SELF_PATH / crate / "Cargo.toml", "rb") as fobj:
        return tomllib.load(fobj)


def check_feature_list(crate, label, actual, expected):
    actual_features = sorted(actual)
    expected_features = sorted(expected)

    if actual_features != expected_features:
        raise ValueError(
            f"Invalid {crate} {label}. "
            f"Expected: {expected_features}, found: {actual_features}"
        )


def check_arrow_feature(crate, config, feature, components, extra_features=()):
    *_, version = feature.partition("-")
    check_feature_list(
        crate,
        f"feature definition for {feature}",
        config["features"][feature],
        [
            *(f"dep:{component}-{version}" for component in components),
            *(feature.format(version=version) for feature in extra_features),
        ],
    )

    for component in components:
        check_dependency(
            crate,
            config,
            f"{component}-{version}",
            {
                "package": component,
                "version": version,
                "optional": True,
                "default-features": False,
            },
        )


def check_dependency(crate, config, name, expected):
    actual = config["dependencies"].get(name)
    if actual is None:
        raise ValueError(f"Missing {crate} dependency {name}")

    if actual != expected:
        raise ValueError(
            f"Invalid {crate} dependency {name}. Expected: {expected}, found: {actual}"
        )


if __name__ == "__main__":
    main()
