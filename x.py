self_path = __import__("pathlib").Path(__file__).parent.resolve()
python = __import__("shlex").quote(__import__("sys").executable)

__effect = lambda effect: lambda func: [func, effect(func.__dict__)][0]
cmd = lambda **kw: __effect(lambda d: d.setdefault("@cmd", {}).update(kw))
arg = lambda *a, **kw: __effect(lambda d: d.setdefault("@arg", []).append((a, kw)))

arrow_features = [
    # arrow-version:insert: "arrow-{version}",
    "arrow-59",
    "arrow-58",
    "arrow-57",
    "arrow-56",
    "arrow-55",
    "arrow-54",
    "arrow-53",
]
default_serde_arrow_features = (arrow_features[0],)
default_marrow_features = ("serde", arrow_features[0])
default_workspace_features = (
    f"serde_arrow/{arrow_features[0]}",
    "marrow/serde",
    f"marrow/{arrow_features[0]}",
)
support_packages = (
    "serde_arrow_bench",
    "serde_arrow_example",
    "serde_arrow_integration",
    "marrow_integration",
)
serde_arrow_components = ("arrow-array", "arrow-schema")
marrow_components = ("arrow-array", "arrow-schema", "arrow-data", "arrow-buffer")

CHECKS_PLACEHOLDER = "<<< checks >>>"

# actions/checkout v7.0.0
ACTION_CHECKOUT = "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0"

# astral-sh/setup-uv v8.2.0
ACTION_SETUP_UV = "astral-sh/setup-uv@fac544c07dec837d0ccb6301d7b5580bf5edae39"

# dtolnay/rust-toolchain branch 1.83.0
ACTION_RUST_TOOLCHAIN = (
    "dtolnay/rust-toolchain@bd41891a8e7f4b8649f6d684415e1a6155fe4e22"
)


def _cargo(
    command,
    *packages,
    features=(),
    extra_args=(),
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


def _feature_check_steps(crate, features):
    for feature in features:
        yield {
            "name": f"Check {crate} {feature}",
            "run": _cargo("check", crate, features=(feature,), all_targets=True),
        }


def _clippy(*packages, features=(), all_features=False, fix=False):
    command = _cargo(
        "clippy",
        *packages,
        features=features,
        all_targets=True,
        all_features=all_features,
    )
    return f"{command} --fix" if fix else command


benchmark_renames = {
    "arrow": "arrow_json::ReaderBuilder",
    "arrow_builder": "arrow builder",
    "serde_arrow_arrow": "serde_arrow::to_arrow",
    "serde_arrow_marrow": "serde_arrow::to_marrow",
}

BENCHMARK_BASELINE = "arrow builder"
README_BENCHMARK_IGNORE_GROUPS = {"json_to_arrow"}


@cmd(help="Run all common development tasks before a commit")
@arg("--backtrace", action="store_true", default=False)
def precommit(backtrace=False):
    update_workflows()

    format()
    check()
    test(backtrace=backtrace)
    serde_arrow_example(backtrace)


@cmd(help="Update the github workflows")
def update_workflows():
    _update_workflow(self_path / ".github" / "workflows" / "test.yml", _workflow_test())

    _update_workflow(
        self_path / ".github" / "workflows" / "release-marrow.yml",
        _release_workflow_template(
            "marrow",
            [
                {
                    "name": "Check marrow",
                    "run": _cargo("check", "marrow", all_targets=True),
                },
                *_feature_check_steps("marrow", ("serde", *arrow_features)),
                {
                    "name": "Test marrow",
                    "run": _cargo("test", "marrow", features=default_marrow_features),
                },
                {
                    "name": "Package marrow",
                    "run": "cargo package -p marrow --allow-dirty",
                },
            ],
        ),
    )

    _update_workflow(
        self_path / ".github" / "workflows" / "release-serde-arrow.yml",
        _release_workflow_template(
            "serde_arrow",
            [
                {
                    "name": "Check serde_arrow",
                    "run": _cargo("check", "serde_arrow", all_targets=True),
                },
                *_feature_check_steps("serde_arrow", arrow_features),
                {
                    "name": "Test serde_arrow",
                    "run": _cargo(
                        "test", "serde_arrow", features=default_serde_arrow_features
                    ),
                },
                {
                    "name": "Package serde_arrow",
                    "run": "cargo package -p serde_arrow --allow-dirty",
                },
            ],
        ),
    )


def _update_workflow(path, workflow):
    import json

    print(f":: update {path}")
    with open(path, "wt", encoding="utf8", newline="\n") as fobj:
        json.dump(workflow, fobj, indent=2)


def _release_workflow_template(crate, check_steps):
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


def _workflow_test():
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
            "build": {
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
                    {"name": "Check", "run": "cargo check"},
                    *_feature_check_steps("marrow", ("serde", *arrow_features)),
                    *_feature_check_steps("serde_arrow", arrow_features),
                    {
                        "name": "Check support packages",
                        "run": _cargo("check", *support_packages, all_features=True),
                    },
                    {
                        "name": "Check format",
                        "run": "cargo fmt --check",
                    },
                    {
                        "name": "Build",
                        "run": _cargo(
                            "build",
                            "serde_arrow",
                            "marrow",
                            features=default_workspace_features,
                        ),
                    },
                    {
                        "name": "Test",
                        "run": _cargo(
                            "test",
                            "serde_arrow",
                            "marrow",
                            features=default_workspace_features,
                        ),
                    },
                    {
                        "name": "Integration test",
                        "run": "uv run python x.py test-integration",
                    },
                ],
            },
        },
    }


@cmd(help="Format the code")
def format():
    _sh(f"{python} -m ruff format {_q(__file__)}")
    _sh("cargo fmt")
    _sh(
        f"""
            rustfmt
                {_q([*self_path.joinpath("marrow", "src", "impl_arrow").glob("impl*.rs")])}
                {_q([*self_path.joinpath("marrow_integration", "src", "tests").glob("*.rs")])}
        """
    )


@cmd(help="Run the linters")
@arg("--all", action="store_true")
@arg("--fix", action="store_true")
def check(all=False, fix=False):
    check_cargo_toml()
    for command in _check_commands(fix=fix, all=all):
        _sh(command)


def _check_commands(fix=False, all=False):
    yield "cargo check"
    yield _cargo(
        "check",
        "serde_arrow",
        features=default_serde_arrow_features,
        all_targets=True,
    )
    yield _cargo("check", "marrow", features=default_marrow_features, all_targets=True)
    yield _clippy("serde_arrow", features=default_serde_arrow_features, fix=fix)
    yield _clippy("marrow", features=default_marrow_features, fix=fix)
    yield _cargo("check", *support_packages, all_targets=True, all_features=True)
    yield _clippy(*support_packages, all_features=True, fix=fix)
    if all:
        for arrow_feature in arrow_features:
            yield _cargo("check", "marrow", features=(arrow_feature,), all_targets=True)

        for arrow_feature in arrow_features:
            yield _cargo(
                "check",
                "serde_arrow",
                features=(arrow_feature,),
                all_targets=True,
            )


@cmd(help="Run the serde_arrow_example")
@arg("--backtrace", action="store_true", default=False)
def serde_arrow_example(backtrace=False):
    _sh(
        "cargo run -p serde_arrow_example",
        env=({"RUST_BACKTRACE": "1"} if backtrace else {}),
    )
    _sh(
        f"{python} -c 'import polars as pl; print(pl.read_ipc(\"serde_arrow_example.ipc\"))'"
    )


@cmd(help="Run both unit and integration tests")
@arg(
    "--backtrace",
    action="store_true",
    default=False,
    help="If given, print a backtrace on error",
)
def test(backtrace=False):
    test_unit(backtrace=backtrace)
    test_integration(backtrace)


@cmd(help="Run the tests")
@arg(
    "--backtrace",
    action="store_true",
    default=False,
    help="If given, print a backtrace on error",
)
@arg(
    "--full",
    action="store_true",
    default=False,
    help="If given, run all feature combinations",
)
@arg("test_name", nargs="?", help="Filter of test names")
def test_unit(test_name=None, backtrace=False, full=False):
    if not full:
        commands = [
            _cargo(
                "test",
                "serde_arrow",
                features=default_serde_arrow_features,
                quiet=True,
            ),
            _cargo("test", "marrow", features=default_marrow_features, quiet=True),
        ]

    else:
        commands = [
            _cargo("test", "marrow", features=("serde",), quiet=True),
            *(
                _cargo("test", "marrow", features=(feature,), quiet=True)
                for feature in arrow_features
            ),
            *(
                _cargo("test", "serde_arrow", features=(feature,), quiet=True)
                for feature in arrow_features
            ),
        ]

    for command in commands:
        _sh(
            f"""
                {command}
                    {_q(test_name) if test_name else ""}
            """,
            env=({"RUST_BACKTRACE": "1"} if backtrace else {}),
        )


@cmd(help="Run integration tests")
@arg(
    "--backtrace",
    action="store_true",
    default=False,
    help="If given, print a backtrace on error",
)
def test_integration(backtrace=False):
    env = {"SERDE_ARROW_PYTHON": __import__("sys").executable}
    if backtrace:
        env["RUST_BACKTRACE"] = "1"

    _sh(
        "cargo test -p serde_arrow_integration",
        env=env,
    )


@cmd()
def check_cargo_toml():
    import tomllib

    def load_config(crate):
        with open(self_path / crate / "Cargo.toml", "rb") as fobj:
            return tomllib.load(fobj)

    def check_feature_list(crate, label, actual, expected):
        actual_features = sorted(actual)
        expected_features = sorted(expected)

        if actual_features != expected_features:
            raise ValueError(
                f"Invalid {crate} {label}. "
                f"Expected: {expected_features}, found: {actual_features}"
            )

    def check_dependency(crate, config, name, expected):
        actual = config["dependencies"].get(name)
        if actual is None:
            raise ValueError(f"Missing {crate} dependency {name}")

        if actual != expected:
            raise ValueError(
                f"Invalid {crate} dependency {name}. "
                f"Expected: {expected}, found: {actual}"
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
        default_serde_arrow_features,
    )
    check_feature_list(
        "marrow",
        "docs.rs configuration",
        marrow_config["package"]["metadata"]["docs"]["rs"]["features"],
        default_marrow_features,
    )

    for feature in arrow_features:
        check_arrow_feature(
            "serde_arrow",
            serde_arrow_config,
            feature,
            serde_arrow_components,
            extra_features=("marrow/arrow-{version}",),
        )
        check_arrow_feature(
            "marrow",
            marrow_config,
            feature,
            marrow_components,
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


@cmd(help="Run the benchmarks")
@arg("--quick", action="store_true", default=False)
def serde_arrow_bench(quick=False):
    _sh(
        "cargo bench -p serde_arrow_bench",
        env=({"SERDE_ARROW_BENCH_QUICK": "1"} if quick else {}),
    )
    summarize_bench()


@cmd(help="Run the bench action for the current branch")
def bench_remote():
    branch = _sh("git branch --show-current", capture_output=True).stdout.strip()
    if not branch:
        raise RuntimeError("Could not determine branch")

    _sh(f"gh workflow run Bench --ref {branch}")


@cmd(help="Summarize the benchmarks")
@arg("--update", action="store_true", default=False)
def summarize_bench(update=False):
    mean_times = load_times()

    print(format_benchmark(mean_times))

    if update:
        update_readme(mean_times, ignore_groups=README_BENCHMARK_IGNORE_GROUPS)
        plot_times(mean_times, ignore_groups=README_BENCHMARK_IGNORE_GROUPS)


def load_times():
    import json
    import statistics

    root = self_path / "target" / "criterion/"

    results = []
    for p in root.glob("*/*/new/sample.json"):
        group = p.parent.parent.parent.name
        name = p.parent.parent.name
        with open(p) as fobj:
            data = json.load(fobj)

        for iterations, time in zip(data["iters"], data["times"]):
            results.append(
                {
                    "name": benchmark_renames.get(name, name),
                    "group": group,
                    "iterations": iterations,
                    "time": time,
                    "seconds_per_iter": time / iterations / 1e9,
                }
            )

    grouped_times = collect(
        ((d["group"], d["name"]), d["seconds_per_iter"]) for d in results
    )

    mean_times = {}
    for k, times in grouped_times.items():
        # remove the top 5% of times
        qq = statistics.quantiles(times, n=20)
        mean_times[k] = statistics.mean(time for time in times if time < qq[-1])

    return mean_times


def update_readme(mean_times, ignore_groups=()):
    print("Update readme")
    with open(self_path / "Readme.md", "rt", encoding="utf8") as fobj:
        lines = [line.rstrip() for line in fobj]

    with open(self_path / "Readme.md", "wt", encoding="utf8", newline="\n") as fobj:
        for line in replace_marked_section(
            lines,
            start_marker="<!-- start:benchmarks -->",
            end_marker="<!-- end:benchmarks -->",
            content=format_benchmark(mean_times, ignore_groups=ignore_groups),
        ):
            print(line, file=fobj)


def replace_marked_section(lines, *, start_marker, end_marker, content):
    start = None
    end = None
    for idx, line in enumerate(lines):
        if line.strip() == start_marker:
            start = idx
        elif line.strip() == end_marker:
            end = idx
            break

    if start is None or end is None or end < start:
        raise RuntimeError(
            f"Could not find marker block {start_marker!r}..{end_marker!r}"
        )

    return [
        *lines[: start + 1],
        *content.splitlines(),
        *lines[end:],
    ]


def plot_times(mean_times, ignore_groups=()):
    print("Plot times")

    import matplotlib.pyplot as plt
    import polars as pl

    df = pl.from_dicts(
        [
            {"group": group, "impl": impl, "time": time}
            for (group, impl), time in mean_times.items()
            if group not in ignore_groups
        ]
    )
    agg_df = (
        df.select(
            [
                pl.col("impl"),
                (
                    pl.col("time")
                    / pl.col("time")
                    .filter(pl.col("impl") == BENCHMARK_BASELINE)
                    .mean()
                    .over("group")
                ),
            ]
        )
        .group_by("impl")
        .agg(pl.col("time").mean())
        .sort("time")
    )

    plt.figure(figsize=(7, 3.5), dpi=150)
    b = plt.barh(
        [d["impl"] for d in agg_df.to_dicts()],
        [d["time"] for d in agg_df.to_dicts()],
        zorder=10,
    )
    plt.bar_label(
        b,
        ["{:.1f} x".format(d["time"]) for d in agg_df.to_dicts()],
        bbox=dict(boxstyle="square,pad=0.0", fc="white", ec="none"),
        padding=2.5,
    )
    plt.grid(axis="x", zorder=0)
    plt.xlim(0, 1.15 * agg_df["time"].max())
    plt.subplots_adjust(left=0.32, right=0.975, top=0.95, bottom=0.15)
    plt.xlabel(f"Mean runtime compared to {BENCHMARK_BASELINE}")
    plt.savefig(self_path / "timings.png")


def format_benchmark(mean_times, ignore_groups=()):
    def _parts():
        for group in sorted({g for g, _ in mean_times if g not in ignore_groups}):
            times_in_group = {n: v for (g, n), v in mean_times.items() if g == group}
            sorted_items = sorted(times_in_group.items(), key=lambda kv: kv[1])

            rows = [["label", "time [ms]", *(k[:15] for k, _ in sorted_items)]]
            for label, time in sorted_items:
                rows.append(
                    [
                        label,
                        f"{1000 * time:7.2f}",
                        *(f"{time / cmp:.2f}" for _, cmp in sorted_items),
                    ]
                )

            widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]

            yield f"### `{group}`"
            yield ""
            for idx, row in enumerate(rows):
                padded_row = [
                    (str.ljust if idx == 0 else str.rjust)(item, width)
                    for idx, (item, width) in enumerate(zip(row, widths))
                ]

                if idx == 0:
                    yield "| " + " | ".join(padded_row) + " |"
                    yield "|-" + "-|-".join("-" * w for w in widths) + "-|"
                else:
                    yield "| " + " | ".join(padded_row) + " |"

            yield ""

    return "\n".join(_parts())


def collect(kv_pairs):
    res = {}
    for k, v in kv_pairs:
        res.setdefault(k, []).append(v)

    return res


@cmd(help="Generate the documentation")
@arg("--private", action="store_true", default=False)
@arg("--open", action="store_true", default=False)
def doc(private=False, open=False):
    _sh(
        _cargo(
            "doc",
            "serde_arrow",
            "marrow",
            features=default_workspace_features,
            extra_args=(("--document-private-items",) if private else ())
            + (("--open",) if open else ()),
        ),
    )


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
_q = lambda arg: (
    __import__("shlex").quote(str(arg))
    if not isinstance(arg, (tuple, list))
    else " ".join(_q(item) for item in arg)
)

if __name__ == "__main__":
    _sps = (_p := __import__("argparse").ArgumentParser()).add_subparsers()
    for _f in (f for _, f in sorted(globals().items()) if hasattr(f, "@cmd")):
        _kw = {"name": _f.__name__.replace("_", "-"), **getattr(_f, "@cmd")}
        (_sp := _sps.add_parser(**_kw)).set_defaults(_=_f)
        [_sp.add_argument(*a, **kw) for a, kw in reversed(getattr(_f, "@arg", []))]
    (_a := vars(_p.parse_args())).pop("_", _p.print_help)(**_a)
