# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
self_path = __import__("pathlib").Path(__file__).parent.resolve()
python = __import__("shlex").quote(__import__("sys").executable)

__effect = lambda effect: lambda func: [func, effect(func.__dict__)][0]
cmd = lambda **kw: __effect(lambda d: d.setdefault("@cmd", {}).update(kw))
arg = lambda *a, **kw: __effect(lambda d: d.setdefault("@arg", []).append((a, kw)))

all_arrow_features = [
    # arrow-version:insert:     "arrow-{version}",
    "arrow-59",
    "arrow-58",
    "arrow-57",
    "arrow-56",
    "arrow-55",
    "arrow-54",
    "arrow-53",
    "arrow-52",
    "arrow-51",
    "arrow-50",
    "arrow-50",
    "arrow-49",
    "arrow-48",
    "arrow-47",
    "arrow-46",
    "arrow-45",
    "arrow-44",
    "arrow-43",
    "arrow-42",
    "arrow-41",
    "arrow-40",
    "arrow-39",
    "arrow-38",
    "arrow-37",
]
all_arrow2_features = ["arrow2-0-17", "arrow2-0-16"]
default_features = f"serde,{all_arrow2_features[0]},{all_arrow_features[0]}"

workflow_test_template = lambda: {
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
        "test": {
            "runs-on": "ubuntu-latest",
            "steps": [
                {"uses": "actions/checkout@v4"},
                *_workflow_check_steps(),
            ],
        }
    },
}

workflow_release_template = lambda: {
    "name": "Release",
    "on": {
        "release": {"types": ["published"]},
    },
    "env": {"CARGO_TERM_COLOR": "always"},
    "jobs": {
        "release": {
            "runs-on": "ubuntu-latest",
            "environment": "release",
            "permissions": {"id-token": "write"},
            "steps": [
                {"uses": "actions/checkout@v4"},
                *_workflow_check_steps(),
                {
                    "name": "Auth with crates.io",
                    "uses": "rust-lang/crates-io-auth-action@v1",
                    "id": "auth",
                },
                {
                    "name": "Publish to crates.io",
                    "working-directory": "marrow",
                    "run": "cargo publish",
                    "env": {
                        "CARGO_REGISTRY_TOKEN": "${{ steps.auth.outputs.token }}",
                    },
                },
            ],
        }
    },
}


@cmd(help="Run all common development tasks before a commit")
@arg("--backtrace", action="store_true", default=False)
def precommit(backtrace=False):
    update_workflows()

    format()
    check()
    test(backtrace=backtrace)


@cmd(help="Update the github workflows")
def update_workflows():
    _update_json_file(
        self_path / ".github" / "workflows" / "test.yml",
        workflow_test_template(),
    )

    _update_json_file(
        self_path / ".github" / "workflows" / "release.yml",
        workflow_release_template(),
    )


def _update_json_file(path, content):
    import json

    print(f":: update {path}")
    with open(path, "wt", encoding="utf8", newline="\n") as fobj:
        json.dump(content, fobj, indent=2)


def _workflow_check_steps():
    return [
        {"name": "system", "run": "uname -a"},
        {"name": "rustc", "run": "rustc --version"},
        {"name": "cargo", "run": "cargo --version"},
        {
            "name": "Check format",
            "run": "cargo fmt --check",
        },
        {"name": "Check", "run": "cargo check"},
        *(
            {
                "name": f"Check {feature}",
                "run": f"cargo check -p marrow --features {feature}",
            }
            for feature in ("serde", *all_arrow2_features, *all_arrow_features)
        ),
        {"name": "Check", "run": "cargo check --all-features"},
        {
            "name": "Build",
            "run": "cargo build --all-features",
        },
        {
            "name": "Test",
            "run": "cargo test --all-features",
        },
    ]


@cmd(help="Format the code")
def format():
    _sh(f"uv run --with 'ruff==0.11.5' ruff format {_q(__file__)}")
    _sh("cargo fmt")

    # the impl files are not found by cargo fmt
    impl_files = [
        *self_path.joinpath("marrow", "src", "impl_arrow").glob("impl*.rs"),
        *self_path.joinpath("marrow", "src", "impl_arrow2").glob("impl*.rs"),
        *self_path.joinpath("test_with_arrow", "src", "tests").glob("*.rs"),
    ]

    _sh(f"rustfmt {_q(impl_files)}")


@cmd(help="Run the linters")
@arg("--all", action="store_true")
def check(all=False):
    check_cargo_toml()
    _sh(f"cargo check --all-features")
    _sh(f"cargo clippy --all-features")

    if all:
        for features in ("serde", *all_arrow2_features, *all_arrow_features):
            _sh(f"cargo check -p marrow --features {features}")


@cmd(help="Run the tests")
@arg(
    "--backtrace",
    action="store_true",
    default=False,
    help="If given, print a backtrace on error",
)
@arg(
    "--all",
    action="store_true",
    default=False,
    help="If given, run all feature combinations",
)
def test(backtrace=False, all=False):
    feature_selection = (
        f"--features {default_features}" if not all else "--all-features"
    )
    env = {"RUST_BACKTRACE": "1"} if backtrace else {}

    _sh("cargo test --features serde", env=env)
    _sh(f"cargo test {feature_selection}", env=env)


@cmd(help="Generate the documentation")
@arg("--private", action="store_true", default=False)
@arg("--open", action="store_true", default=False)
def doc(private=False, open=False):
    _sh(
        f"""
            cargo doc
                --features {default_features}
                {"--document-private-items" if private else ""}
                {"--open" if open else ""}
        """,
    )


@cmd()
def check_cargo_toml():
    import tomllib

    print(":: check Cargo.toml")
    with open(self_path / "marrow" / "Cargo.toml", "rb") as fobj:
        config = tomllib.load(fobj)

    for label, features in [
        (
            "docs.rs configuration",
            config["package"]["metadata"]["docs"]["rs"]["features"],
        ),
        *[
            (f"test {target['name']}", target["required-features"])
            for target in config.get("test", [])
        ],
        *[
            (f"bench {target['name']}", target["required-features"])
            for target in config.get("bench", [])
        ],
    ]:
        actual_features = sorted(features)
        expected_features = sorted(default_features.split(","))

        if actual_features != expected_features:
            raise ValueError(
                f"Invalid {label}. "
                f"Expected: {expected_features}, found: {actual_features}"
            )

    # TODO: check the features / dependencies
    for feature in all_arrow_features:
        *_, version = feature.partition("-")

        actual_feature_def = sorted(config["features"][feature])
        expected_feature_def = sorted(
            [
                f"dep:arrow-array-{version}",
                f"dep:arrow-schema-{version}",
                f"dep:arrow-data-{version}",
                f"dep:arrow-buffer-{version}",
            ]
        )

        if actual_feature_def != expected_feature_def:
            raise ValueError(
                f"Invalid feature definition for {feature}. "
                f"Expected: {expected_feature_def}, found: {actual_feature_def}"
            )

        for component in ["arrow-array", "arrow-schema", "arrow-data", "arrow-buffer"]:
            expected_dep = {
                "package": component,
                "version": version,
                "optional": True,
                "default-features": False,
            }
            actual_dep = config["dependencies"].get(f"{component}-{version}")

            if actual_dep is None:
                raise ValueError(f"Missing dependency {component}-{version}")

            if actual_dep != expected_dep:
                raise ValueError(
                    f"Invalid dependency {component}-{version}. "
                    f"Expected: {expected_dep}, found: {actual_dep}"
                )

        for name, dep in config["dependencies"].items():
            if dep.get("default-features", True):
                raise ValueError(f"Default features for {name} not deactivated")


@cmd(help="Add a new arrow version")
@arg("version")
def add_arrow_version(version):
    import re

    if _sh("git diff-files --quiet", check=False).returncode != 0:
        print(
            "WARNING: potentially destructive changes. "
            "Please stage or commit the working tree first."
        )
        raise SystemExit(1)

    for p in [
        self_path / "x.py",
        *self_path.glob("*/**/*.rs"),
        *self_path.glob("*/**/*.toml"),
    ]:
        content = p.read_text()
        if "arrow-version" not in content:
            continue

        print(f"process {p}")
        new_content = []
        include_next = True
        for line in content.splitlines():
            if (
                m := re.match(r"^.*(//|#) arrow-version:(replace|insert): (.*)$", line)
            ) is not None:
                new_content.append(line)
                new_content.append(
                    m.group(3).format_map({"version": version, "\\n": "\n"})
                )
                include_next = m.group(2) != "replace"

            else:
                if include_next:
                    new_content.append(line)

                include_next = True

        with open(p, "wt", newline="\n", encoding="utf-8") as fobj:
            fobj.write("\n".join(new_content))

    format()
    update_workflows()


_sh = lambda c, env=(), **kw: __import__("subprocess").run(
    [args := __import__("shlex").split(c.replace("\n", " ")), print("::", *args)][0],
    **{
        "check": True,
        "cwd": self_path,
        "encoding": "utf-8",
        "env": {**__import__("os").environ, **dict(env)},
        **kw,
    },
)
_q = (
    lambda a: __import__("shlex").quote(str(a))
    if not isinstance(a, (tuple, list))
    else " ".join(_q(i) for i in a)
)


if __name__ == "__main__":
    _sps = (_p := __import__("argparse").ArgumentParser()).add_subparsers()
    for _f in (f for _, f in sorted(globals().items()) if hasattr(f, "@cmd")):
        _kw = {"name": _f.__name__.replace("_", "-"), **getattr(_f, "@cmd")}
        (_sp := _sps.add_parser(**_kw)).set_defaults(_=_f)
        [_sp.add_argument(*a, **kw) for a, kw in reversed(getattr(_f, "@arg", []))]
    (_a := vars(_p.parse_args())).pop("_", _p.print_help)(**_a)
