import argparse
import copy
import io
import itertools as it
import json
import os
import pathlib
import re
import statistics
import subprocess
import sys

self_path = pathlib.Path(__file__).parent.resolve()

_md = lambda effect: lambda f: [f, effect(f)][0]
_ps = lambda o: vars(o).setdefault("__chmp__", {})
_as = lambda o: _ps(o).setdefault("__args__", [])
cmd = lambda **kw: _md(lambda f: _ps(f).update(kw))
arg = lambda *a, **k: _md(lambda f: _as(f).insert(0, (a, k)))


all_arrow_features = ["arrow-35", "arrow-36", "arrow-37", "arrow-38", "arrow-39"]
all_arrow2_features = ["arrow2-0-16", "arrow2-0-17"]
default_features = f"{all_arrow2_features[-1]},{all_arrow_features[-1]}"

workflow_test_template = {
    "name": "Test",
    "on": {
        "workflow_dispatch": {},
        "push": {},
        "pull_request": {"branches": ["main"], "types": ["ready_for_review"]},
    },
    "env": {"CARGO_TERM_COLOR": "always"},
    "jobs": {
        "build": {
            "runs-on": "ubuntu-latest",
            "steps": [
                {"uses": "actions/checkout@v3"},
                {"name": "rustc", "run": "rustc --version"},
                {"name": "cargo", "run": "cargo --version"},
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
            ],
        }
    },
}


@cmd()
@arg("--backtrace", action="store_true", default=False)
def precommit(backtrace=False):
    run(
        sys.executable,
        self_path / "serde_arrow" / "src" / "arrow2" / "gen_display_tests.py",
    )
    generate_workflows()

    fmt()
    check()
    lint()
    test(backtrace=backtrace)
    example()


def generate_workflows():
    workflow_test = copy.deepcopy(workflow_release_template)
    _add_workflow_check_steps(workflow_test["jobs"]["build"]["steps"])

    path = self_path / ".github" / "workflows" / "test.yml"
    print(f":: update {path}")
    with open(path, "wt", encoding="utf8") as fobj:
        json.dump(workflow_test, fobj, indent=2)

    workflow_release = copy.deepcopy(workflow_release_template)
    _add_workflow_check_steps(workflow_release["jobs"]["build"]["steps"])
    workflow_release["jobs"]["build"]["steps"].append(
        {
            "name": "Publish to crates.io",
            "working-directory": "serde_arrow",
            "run": "cargo publish",
        }
    )

    path = self_path / ".github" / "workflows" / "release.yml"
    print(f":: update {path}")
    with open(path, "wt", encoding="utf8") as fobj:
        json.dump(workflow_release, fobj, indent=2)


def _add_workflow_check_steps(steps):
    steps.append({"name": "Check", "run": "cargo check --verbose"})
    for feature in (*all_arrow2_features, *all_arrow_features):
        steps.append(
            {
                "name": f"Check {feature}",
                "run": f"cargo check --verbose --features {feature}",
            }
        )

    steps.append(
        {"name": "Build", "run": f"cargo build --verbose --features {default_features}"}
    )
    steps.append(
        {"name": "Build", "run": f"cargo test --verbose --features {default_features}"}
    )


@cmd()
def fmt():
    cargo("fmt")


@cmd()
def check():
    cargo("check")
    for arrow2_feature in (*all_arrow2_features, *all_arrow_features):
        cargo(
            "check",
            "--features",
            arrow2_feature,
        )


@cmd()
def lint():
    check_cargo_toml()
    check_rust_cfg()
    cargo("clippy", "--features", default_features)


@cmd()
def example():
    cargo("run", "-p", "example")


@cmd()
@arg("--backtrace", action="store_true", default=False)
@arg("--full", action="store_true", default=False)
def test(backtrace=False, full=False):
    if not full:
        flag_combinations = [["--features", default_features]]

    else:
        flag_combinations = []
        for arrow_feature in [[], *([feat] for feat in all_arrow_features)]:
            for arrow2_feature in [[], *([feat] for feat in all_arrow2_features)]:
                if not arrow_feature and not arrow2_feature:
                    flag_combinations.append([])

                else:
                    flag_combinations.append(
                        ["--features", ",".join(arrow_feature + arrow2_feature)]
                    )

    for flags in flag_combinations:
        cargo(
            "test",
            *flags,
            env=dict(os.environ, RUST_BACKTRACE="1" if backtrace else "0"),
        )


@cmd()
def check_cargo_toml():
    import toml

    with open(self_path / "serde_arrow" / "Cargo.toml", "rt") as fobj:
        config = toml.load(fobj)

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
                f"Expected features {expected_features}, found: {actual_features}"
            )


@cmd()
def check_rust_cfg():
    pass


@cmd()
def bench():
    cargo("bench", "--features", default_features)
    summarize_bench()


@cmd()
@arg("--update", action="store_true", default=False)
def summarize_bench(update=False):
    mean_times = load_times()

    print(format_benchmark(mean_times))

    if update:
        update_readme(mean_times)
        plot_times(mean_times)


def load_times():
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
                    "name": name,
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


def update_readme(mean_times):
    print("Update readme")
    with open(self_path / "Readme.md", "rt", encoding="utf8") as fobj:
        lines = [line.rstrip() for line in fobj]

    active = False
    with open(self_path / "Readme.md", "wt", encoding="utf8") as fobj:
        for line in lines:
            if not active:
                print(line, file=fobj)
                if line.strip() == "<!-- start:benchmarks -->":
                    active = True

            else:
                if line.strip() == "<!-- end:benchmarks -->":
                    print(format_benchmark(mean_times), file=fobj)
                    print(line, file=fobj)
                    active = False


def plot_times(mean_times):
    print("Plot times")

    import matplotlib.pyplot as plt
    import polars as pl

    df = pl.from_dicts(
        [
            {"group": group, "impl": impl, "time": time}
            for (group, impl), time in mean_times.items()
        ]
    )
    agg_df = (
        df.select(
            [
                pl.col("impl"),
                (
                    pl.col("time")
                    / pl.col("time")
                    .where(pl.col("impl") == "arrow2_convert")
                    .mean()
                    .over("group")
                ),
            ]
        )
        .groupby("impl")
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
    plt.subplots_adjust(left=0.25, right=0.95, top=0.95, bottom=0.15)
    plt.xlabel("Mean runtime compared to arrow2_convert")
    plt.savefig(self_path / "timings.png")


def format_benchmark(mean_times):
    with io.StringIO() as fobj:
        for group in sorted({g for g, _ in mean_times}):
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

            print("### ", group, file=fobj)
            print(file=fobj)
            for idx, row in enumerate(rows):
                padded_row = [
                    (str.ljust if idx == 0 else str.rjust)(item, width)
                    for idx, item, width in zip(it.count(), row, widths)
                ]

                if idx == 0:
                    print("| " + " | ".join(padded_row) + " |", file=fobj)
                    print("|-" + "-|-".join("-" * w for w in widths) + "-|", file=fobj)
                else:
                    print("| " + " | ".join(padded_row) + " |", file=fobj)

            print(file=fobj)

        return fobj.getvalue()


@cmd()
def summarize_status():
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
    num_ignored_tests = _count_pattern(r"^\s*[ignore]\s*$")
    num_no_compilation = _count_pattern(r"^\s*test_compilation\s*=\s*\[\s*\]\s*,\s*$")
    num_no_deserialization = _count_pattern(
        r"^\s*test_deserialization\s*=\s*false\s*,\s*$"
    )

    print("tests:                  ", num_tests)
    print("ignored tests:          ", num_ignored_tests)
    for (label, num_false) in [
        ("compilation support:    ", num_no_compilation),
        ("deserialization support:", num_no_deserialization),
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


@cmd()
@arg("--private", action="store_true", default=False)
def doc(private=False):
    cargo(
        "doc",
        "--features",
        default_features,
        *(["--document-private-items"] if private else []),
        cwd=self_path / "serde_arrow",
    )


def cargo(*args, **kwargs):
    return run("cargo", *args, **kwargs)


def run(*args, **kwargs):
    kwargs.setdefault("check", True)

    args = [str(arg) for arg in args]
    print("::", " ".join(args))
    return subprocess.run(args, **kwargs)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    for func in globals().values():
        if not hasattr(func, "__chmp__"):
            continue

        desc = dict(func.__chmp__)
        name = desc.pop("name", func.__name__.replace("_", "-"))
        args = desc.pop("__args__", [])

        subparser = subparsers.add_parser(name, **desc)
        subparser.set_defaults(__main__=func)

        for arg_args, arg_kwargs in args:
            subparser.add_argument(*arg_args, **arg_kwargs)

    args = vars(parser.parse_args())
    return args.pop("__main__")(**args) if "__main__" in args else parser.print_help()


if __name__ == "__main__":
    main()
