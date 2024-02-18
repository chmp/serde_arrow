self_path = __import__("pathlib").Path(__file__).parent.resolve()
python = __import__("shlex").quote(__import__("sys").executable)

__effect = lambda effect: lambda func: [func, effect(func.__dict__)][0]
cmd = lambda **kw: __effect(lambda d: d.setdefault("@cmd", {}).update(kw))
arg = lambda *a, **kw: __effect(lambda d: d.setdefault("@arg", []).append((a, kw)))

all_arrow_features = [
    # arrow-version:insert: "arrow-{version}",
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
default_features = f"{all_arrow2_features[0]},{all_arrow_features[0]}"

CHECKS_PLACEHOLDER = "<<< checks >>>"

workflow_test_template = {
    "name": "Test",
    "on": {
        "workflow_dispatch": {},
        "pull_request": {
            "branches": ["main"],
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
        "build": {
            "runs-on": "ubuntu-latest",
            "steps": [
                {"uses": "actions/checkout@v3"},
                {"name": "rustc", "run": "rustc --version"},
                {"name": "cargo", "run": "cargo --version"},
                CHECKS_PLACEHOLDER,
            ],
        }
    },
}

workflow_release_template = {
    "name": "Release",
    "on": {
        "release": {"types": ["published"]},
    },
    "env": {"CARGO_TERM_COLOR": "always"},
    "jobs": {
        "build": {
            "runs-on": "ubuntu-latest",
            "env": {
                "CARGO_REGISTRY_TOKEN": "${{ secrets.CARGO_REGISTRY_TOKEN }}",
            },
            "steps": [
                {"uses": "actions/checkout@v3"},
                {"name": "rustc", "run": "rustc --version"},
                {"name": "cargo", "run": "cargo --version"},
                CHECKS_PLACEHOLDER,
                {
                    "name": "Publish to crates.io",
                    "working-directory": "serde_arrow",
                    "run": "cargo publish",
                },
            ],
        }
    },
}

benchmark_renames = {
    "arrow": "arrow_json::ReaderBuilder",
    "serde_arrow_arrow": "serde_arrow::to_arrow",
    "serde_arrow_arrow2": "serde_arrow::to_arrow2",
    "arrow2_convert": "arrow2_convert::TryIntoArrow",
}


@cmd(help="Run all common development tasks before a commit")
@arg("--backtrace", action="store_true", default=False)
def precommit(backtrace=False):
    update_workflows()

    format()
    check()
    test(backtrace=backtrace)
    example()


@cmd(help="Update the github workflows")
def update_workflows():
    _update_workflow(
        self_path / ".github" / "workflows" / "test.yml",
        workflow_test_template,
    )

    _update_workflow(
        self_path / ".github" / "workflows" / "release.yml",
        workflow_release_template,
    )


def _update_workflow(path, template):
    import copy, json

    workflow = copy.deepcopy(template)

    for job in workflow["jobs"].values():
        steps = []
        for step in job["steps"]:
            if step == CHECKS_PLACEHOLDER:
                steps.extend(_generate_workflow_check_steps())

            else:
                assert isinstance(step, dict)
                steps.append(step)

        job["steps"] = steps

    print(f":: update {path}")
    with open(path, "wt", encoding="utf8", newline="\n") as fobj:
        json.dump(workflow, fobj, indent=2)


def _generate_workflow_check_steps():
    yield {"name": "Check", "run": "cargo check"}
    for feature in (*all_arrow2_features, *all_arrow_features):
        yield {
            "name": f"Check {feature}",
            "run": f"cargo check --features {feature}",
        }

    yield {
        "name": "Build",
        "run": f"cargo build --features {default_features}",
    }
    yield {
        "name": "Test",
        "run": f"cargo test --features {default_features}",
    }


@cmd(help="Format the code")
def format():
    _sh(f"{python} -m black {_q(__file__)}")
    _sh("cargo fmt")


@cmd(help="Run the linters")
@arg("--fast", action="store_true")
def check(fast=False):
    check_cargo_toml()
    _sh(f"cargo check --features {default_features}")
    _sh(f"cargo clippy --features {default_features}")

    if not fast:
        for arrow2_feature in (*all_arrow2_features, *all_arrow_features):
            _sh(f"cargo check --features {arrow2_feature}")


@cmd(help="Run the example")
def example():
    _sh("cargo run -p example")
    _sh(f"{python} -c 'import polars as pl; print(pl.read_ipc(\"example.ipc\"))'")


@cmd(help="Run the tests")
@arg("--backtrace", action="store_true", default=False)
@arg("--full", action="store_true", default=False)
def test(backtrace=False, full=False):
    import os

    if not full:
        feature_selections = [f"--features {default_features}"]

    else:
        feature_selections = [
            f"--features {', '.join(arrow_feature + arrow2_feature)}"
            if arrow_feature or arrow2_feature
            else ""
            for arrow_feature in [[], *([feat] for feat in all_arrow_features)]
            for arrow2_feature in [[], *([feat] for feat in all_arrow2_features)]
        ]

    for feature_selection in feature_selections:
        _sh(
            f"cargo test {feature_selection}",
            env=dict(os.environ, RUST_BACKTRACE="1" if backtrace else "0"),
        )


@cmd()
def check_cargo_toml():
    import tomli

    print(":: check Cargo.toml")
    with open(self_path / "serde_arrow" / "Cargo.toml", "rb") as fobj:
        config = tomli.load(fobj)

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


@cmd(help="Run the benchmarks")
def bench():
    _sh(f"cargo bench --features {default_features}")
    summarize_bench()


@cmd(help="Summarize the benchmarks")
@arg("--update", action="store_true", default=False)
def summarize_bench(update=False):
    mean_times = load_times()

    print(format_benchmark(mean_times))

    if update:
        update_readme(mean_times, ignore_groups={"json_to_arrow"})
        plot_times(mean_times, ignore_groups={"json_to_arrow"})


def load_times():
    import json, statistics

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

    active = False
    with open(self_path / "Readme.md", "wt", encoding="utf8", newline="\n") as fobj:
        for line in lines:
            if not active:
                print(line, file=fobj)
                if line.strip() == "<!-- start:benchmarks -->":
                    active = True

            else:
                if line.strip() == "<!-- end:benchmarks -->":
                    print(
                        format_benchmark(mean_times, ignore_groups=ignore_groups),
                        file=fobj,
                    )
                    print(line, file=fobj)
                    active = False


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
                    .where(
                        pl.col("impl")
                        == benchmark_renames.get("arrow2_convert", "arrow2_convert")
                    )
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
    plt.xlabel("Mean runtime compared to arrow2_convert")
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

            yield f"### {group}"
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


@cmd(help="Summarize to-do items and unimplemented tests")
def summarize_status():
    import re

    def _extract(pat):
        return list(
            m.groups()
            for p in self_path.glob("serde_arrow/src/test_impls/**/*.rs")
            for line in p.read_text(encoding="utf8").splitlines()
            if (m := re.match(pat, line)) is not None
        )

    def _count_pattern(pat):
        return len(_extract(pat))

    num_tests = _count_pattern(r"^\s*test_example!\(\s*$")
    num_ignored_tests = _count_pattern(r"^\s*#[ignore[^\]]*]\s*$")
    num_no_compilation = _count_pattern(r"^\s*test_compilation\s*=\s*\[\s*\]\s*,\s*$")
    num_no_deserialization = _count_pattern(
        r"^\s*test_bytecode_deserialization\s*=\s*false\s*,\s*$"
    )

    print("tests:                  ", num_tests)
    print("ignored tests:          ", num_ignored_tests)
    for label, num_false in [
        ("compilation support:    ", num_no_compilation),
        ("bytecode deser. support:", num_no_deserialization),
    ]:
        print(
            label,
            num_tests - num_false,
            "/",
            num_tests,
            f"({(num_tests - num_false) / num_tests:.0%})",
        )

    print()
    print("# Todo comments:")
    for p in self_path.glob("serde_arrow/**/*.rs"):
        for line in p.read_text(encoding="utf8").splitlines():
            if "todo" in line.lower():
                print(line.strip())


def collect(kv_pairs):
    res = {}
    for k, v in kv_pairs:
        res.setdefault(k, []).append(v)

    return res


def flatten(i):
    for ii in i:
        yield from ii


@cmd(help="Generate the documentation")
@arg("--private", action="store_true", default=False)
def doc(private=False):
    _sh(
        f"cargo doc --features {default_features} {'--document-private-items' if private else ''}",
        cwd=self_path / "serde_arrow",
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
        *self_path.glob("serde_arrow/**/*.rs"),
        *self_path.glob("serde_arrow/**/*.toml"),
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


_sh = lambda c, **kw: __import__("subprocess").run(
    [args := __import__("shlex").split(c.replace("\n", " ")), print("::", *args)][0],
    **{"check": True, "cwd": self_path, "encoding": "utf-8", **kw},
)
_q = lambda arg: __import__("shlex").quote(str(arg))

if __name__ == "__main__":
    _sps = (_p := __import__("argparse").ArgumentParser()).add_subparsers()
    for _f in (f for _, f in sorted(globals().items()) if hasattr(f, "@cmd")):
        _kw = {"name": _f.__name__.replace("_", "-"), **getattr(_f, "@cmd")}
        (_sp := _sps.add_parser(**_kw)).set_defaults(_=_f)
        [_sp.add_argument(*a, **kw) for a, kw in reversed(getattr(_f, "@arg", []))]
    (_a := vars(_p.parse_args())).pop("_", _p.print_help)(**_a)
