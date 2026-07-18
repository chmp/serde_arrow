import argparse
import json
import pathlib
import shlex


SELF_PATH = pathlib.Path(__file__).parents[1].resolve()

# actions/checkout v7.0.0
ACTION_CHECKOUT = "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0"

# astral-sh/setup-uv v8.2.0
ACTION_SETUP_UV = "astral-sh/setup-uv@fac544c07dec837d0ccb6301d7b5580bf5edae39"

# dtolnay/rust-toolchain branch 1.83.0
ACTION_RUST_TOOLCHAIN = (
    "dtolnay/rust-toolchain@bd41891a8e7f4b8649f6d684415e1a6155fe4e22"
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--arrow-features", required=True, nargs="+")
    update_workflows(parser.parse_args())


def update_workflows(args):
    local_workflow = workflow_local(args)
    test_workflow = workflow_test(args)
    release_marrow_workflow = release_workflow_template(
        args,
        "marrow",
        [
            {
                "name": "Check marrow",
                "run": cargo("check", packages=("marrow",), all_targets=True),
            },
            *(
                {
                    "name": f"Check marrow {feature}",
                    "run": cargo(
                        "check",
                        packages=("marrow",),
                        features=(feature,),
                        all_targets=True,
                    ),
                }
                for feature in ("serde", *args.arrow_features)
            ),
            {
                "name": "Test marrow",
                "run": cargo(
                    "test",
                    packages=("marrow",),
                    features=("serde", args.arrow_features[0]),
                ),
            },
            {
                "name": "Package marrow",
                "run": "cargo package -p marrow --allow-dirty",
            },
        ],
    )
    release_serde_arrow_workflow = release_workflow_template(
        args,
        "serde_arrow",
        [
            {
                "name": "Check serde_arrow",
                "run": cargo(
                    "check",
                    packages=("serde_arrow",),
                    all_targets=True,
                ),
            },
            *(
                {
                    "name": f"Check serde_arrow {feature}",
                    "run": cargo(
                        "check",
                        packages=("serde_arrow",),
                        features=(feature,),
                        all_targets=True,
                    ),
                }
                for feature in args.arrow_features
            ),
            {
                "name": "Test serde_arrow",
                "run": cargo(
                    "test",
                    packages=("serde_arrow",),
                    features=(args.arrow_features[0],),
                ),
            },
            {
                "name": "Package serde_arrow",
                "run": "cargo package -p serde_arrow --allow-dirty",
            },
        ],
    )

    update_workflow(SELF_PATH / ".github" / "workflows" / "local.yml", local_workflow)
    update_workflow(SELF_PATH / ".github" / "workflows" / "test.yml", test_workflow)
    update_workflow(
        SELF_PATH / ".github" / "workflows" / "release-marrow.yml",
        release_marrow_workflow,
    )
    update_workflow(
        SELF_PATH / ".github" / "workflows" / "release-serde-arrow.yml",
        release_serde_arrow_workflow,
    )


def workflow_local(args):
    return {
        "name": "Local",
        "on": {
            "workflow_dispatch": {},
        },
        "env": {"CARGO_TERM_COLOR": "always"},
        "jobs": {
            "format": {
                "runs-on": "ubuntu-latest",
                "steps": [
                    *local_setup_steps(),
                    {"name": "Format Python", "run": "uv run ruff format x.py"},
                    {"name": "Format Cargo", "run": "cargo fmt"},
                    {
                        "name": "Format generated Rust",
                        "run": rustfmt_generated_command(),
                    },
                ],
            },
            "check": {
                "runs-on": "ubuntu-latest",
                "steps": [
                    *local_setup_steps(),
                    {
                        "name": "Check Cargo.toml",
                        "run": check_cargo_toml_command(args),
                    },
                    {"name": "Check workspace", "run": "cargo check"},
                    {
                        "name": "Check serde_arrow",
                        "run": cargo(
                            "check",
                            packages=("serde_arrow",),
                            features=(args.arrow_features[0],),
                            all_targets=True,
                        ),
                    },
                    {
                        "name": "Check marrow",
                        "run": cargo(
                            "check",
                            packages=("marrow",),
                            features=("serde", args.arrow_features[0]),
                            all_targets=True,
                        ),
                    },
                    {
                        "name": "Clippy serde_arrow",
                        "run": cargo(
                            "clippy",
                            packages=("serde_arrow",),
                            features=(args.arrow_features[0],),
                            all_targets=True,
                        ),
                    },
                    {
                        "name": "Clippy marrow",
                        "run": cargo(
                            "clippy",
                            packages=("marrow",),
                            features=("serde", args.arrow_features[0]),
                            all_targets=True,
                        ),
                    },
                    {
                        "name": "Check support packages",
                        "run": cargo(
                            "check",
                            packages=(
                                "serde_arrow_bench",
                                "serde_arrow_example",
                                "serde_arrow_integration",
                                "marrow_integration",
                            ),
                            all_targets=True,
                            all_features=True,
                        ),
                    },
                    {
                        "name": "Clippy support packages",
                        "run": cargo(
                            "clippy",
                            packages=(
                                "serde_arrow_bench",
                                "serde_arrow_example",
                                "serde_arrow_integration",
                                "marrow_integration",
                            ),
                            all_targets=True,
                            all_features=True,
                        ),
                    },
                ],
            },
            "test": {
                "runs-on": "ubuntu-latest",
                "steps": [
                    *local_setup_steps(),
                    {
                        "name": "Test serde_arrow",
                        "run": cargo(
                            "test",
                            packages=("serde_arrow",),
                            features=(args.arrow_features[0],),
                            quiet=True,
                        ),
                    },
                    {
                        "name": "Test marrow",
                        "run": cargo(
                            "test",
                            packages=("marrow",),
                            features=("serde", args.arrow_features[0]),
                            quiet=True,
                        ),
                    },
                    {
                        "name": "Integration test",
                        "run": integration_test_command(),
                    },
                ],
            },
            "example": {
                "runs-on": "ubuntu-latest",
                "steps": [
                    *local_setup_steps(),
                    {
                        "name": "Run example",
                        "run": "cargo run -p serde_arrow_example",
                    },
                    {
                        "name": "Read example output",
                        "run": "uv run python -c 'import polars as pl; print(pl.read_ipc(\"serde_arrow_example.ipc\"))'",
                    },
                ],
            },
        },
    }


def local_setup_steps():
    return [
        {"uses": ACTION_CHECKOUT},
        {
            "name": "Install uv",
            "uses": ACTION_SETUP_UV,
            "with": {"enable-cache": True},
        },
        {"name": "rustc", "run": "rustc --version"},
        {"name": "cargo", "run": "cargo --version"},
    ]


def workflow_test(args):
    return {
        "name": "Test",
        "on": {
            "workflow_dispatch": {},
            "pull_request": {
                "branches": ["main", "develop-*"],
                "types": [
                    "opened",
                    "edited",
                    "reopened",
                    "ready_for_review",
                    "synchronize",
                ],
            },
        },
        "env": {"CARGO_TERM_COLOR": "always"},
        "jobs": {
            "msrv": {
                "runs-on": "ubuntu-latest",
                "steps": [
                    {"uses": ACTION_CHECKOUT},
                    {
                        "name": "Install Rust 1.83",
                        "uses": ACTION_RUST_TOOLCHAIN,
                        "with": {"toolchain": "1.83.0"},
                    },
                    {"name": "rustc", "run": "rustc --version"},
                    {"name": "cargo", "run": "cargo --version"},
                    {
                        "name": "Update dependencies",
                        "run": "cargo +stable update --config 'resolver.incompatible-rust-versions=\"fallback\"'",
                    },
                    {
                        "name": "Check MSRV with arrow-53",
                        "run": "cargo +1.83.0 check --package serde_arrow --features arrow-53",
                    },
                ],
            },
            "test": {
                "runs-on": "ubuntu-latest",
                "steps": [
                    {"uses": ACTION_CHECKOUT},
                    {
                        "name": "Install uv",
                        "uses": ACTION_SETUP_UV,
                        "with": {"enable-cache": True},
                    },
                    {"name": "rustc", "run": "rustc --version"},
                    {"name": "cargo", "run": "cargo --version"},
                    {
                        "name": "Check Cargo.toml",
                        "run": check_cargo_toml_command(args),
                    },
                    {"name": "Check", "run": "cargo check"},
                    *(
                        {
                            "name": f"Check marrow {feature}",
                            "run": cargo(
                                "check",
                                packages=("marrow",),
                                features=(feature,),
                                all_targets=True,
                            ),
                        }
                        for feature in ("serde", *args.arrow_features)
                    ),
                    *(
                        {
                            "name": f"Check serde_arrow {feature}",
                            "run": cargo(
                                "check",
                                packages=("serde_arrow",),
                                features=(feature,),
                                all_targets=True,
                            ),
                        }
                        for feature in args.arrow_features
                    ),
                    {
                        "name": "Check support packages",
                        "run": cargo(
                            "check",
                            packages=(
                                "serde_arrow_bench",
                                "serde_arrow_example",
                                "serde_arrow_integration",
                                "marrow_integration",
                            ),
                            all_features=True,
                        ),
                    },
                    {
                        "name": "Check format",
                        "run": "cargo fmt --check",
                    },
                    {
                        "name": "Build",
                        "run": cargo(
                            "build",
                            packages=("serde_arrow", "marrow"),
                            features=(
                                f"serde_arrow/{args.arrow_features[0]}",
                                "marrow/serde",
                                f"marrow/{args.arrow_features[0]}",
                            ),
                        ),
                    },
                    {
                        "name": "Test",
                        "run": cargo(
                            "test",
                            packages=("serde_arrow", "marrow"),
                            features=(
                                f"serde_arrow/{args.arrow_features[0]}",
                                "marrow/serde",
                                f"marrow/{args.arrow_features[0]}",
                            ),
                        ),
                    },
                    {
                        "name": "Integration test",
                        "run": integration_test_command(),
                    },
                ],
            },
        },
    }


def release_workflow_template(args, crate, check_steps):
    return {
        "name": f"Release {crate}",
        "on": {
            "push": {"tags": [f"{crate}/v*"]},
        },
        "env": {"CARGO_TERM_COLOR": "always"},
        "jobs": {
            "publish": {
                "runs-on": "ubuntu-latest",
                "environment": "release",
                "permissions": {"id-token": "write"},
                "steps": [
                    {"uses": ACTION_CHECKOUT},
                    {"name": "rustc", "run": "rustc --version"},
                    {"name": "cargo", "run": "cargo --version"},
                    *check_steps,
                    {
                        "name": "Auth with crates.io",
                        "uses": "rust-lang/crates-io-auth-action@v1",
                        "id": "auth",
                    },
                    {
                        "name": "Publish to crates.io",
                        "run": f"cargo publish -p {crate}",
                        "env": {
                            "CARGO_REGISTRY_TOKEN": "${{ steps.auth.outputs.token }}",
                        },
                    },
                ],
            },
        },
    }


def update_workflow(path, workflow):
    print(f":: update {path}")
    with open(path, "wt", encoding="utf8", newline="\n") as fobj:
        json.dump(workflow, fobj, indent=2)


def check_cargo_toml_command(args):
    return shell_args(
        [
            "uv",
            "run",
            "python",
            "scripts/check-cargo-toml.py",
            "--arrow-features",
            *args.arrow_features,
        ]
    )


def integration_test_command():
    return "cargo test -p serde_arrow_integration"


def rustfmt_generated_command():
    paths = [
        *SELF_PATH.joinpath("marrow", "src", "impl_arrow").glob("impl*.rs"),
        *SELF_PATH.joinpath("marrow_integration", "src", "tests").glob("*.rs"),
    ]
    return shell_args(
        ["rustfmt", *(path.relative_to(SELF_PATH) for path in sorted(paths))]
    )


def cargo(
    command,
    *extra_args,
    packages=(),
    features=(),
    all_targets=False,
    all_features=False,
    quiet=False,
):
    return " ".join(
        part
        for part in [
            "cargo",
            command,
            "-q" if quiet else "",
            "--all-targets" if all_targets else "",
            "--all-features" if all_features else "",
            *(f"--package {package}" for package in packages),
            "--features" if features else "",
            ",".join(features) if features else "",
            *extra_args,
        ]
        if part
    )


def shell_args(args):
    return " ".join(shlex.quote(str(arg)) for arg in args)


if __name__ == "__main__":
    main()
