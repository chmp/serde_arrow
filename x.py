import argparse
import collections
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


all_arrow_features = ["arrow-36", "arrow-37", "arrow-38"]
all_arrow2_features = ["arrow2-0-16", "arrow2-0-17"]
default_features = f"{all_arrow2_features[-1]},{all_arrow_features[-1]}"


@cmd()
@arg("--backtrace", action="store_true", default=False)
def precommit(backtrace=False):
    run(
        sys.executable,
        self_path / "serde_arrow" / "src" / "arrow2" / "gen_display_tests.py",
    )

    fmt()
    check()
    lint()
    test(backtrace=backtrace)
    example()


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
    cargo("clippy", "--features", default_features)


@cmd()
def example():
    cargo("run", "-p", "example")


@cmd()
@arg("--backtrace", action="store_true", default=False)
def test(backtrace=False):
    # TODO: include other feature flag combinations?
    cargo(
        "test",
        "--features",
        default_features,
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
def bench():
    cargo("bench", "--features", default_features)
    summarize_bench()


@cmd()
@arg("--update", action="store_true", default=False)
def summarize_bench(update=False):
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

    print(format_benchmark(mean_times))

    if update:
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
def summarize_progress():
    pat = r"^\s*test_compilation\s*=\s*(true|false)\s*,\s*$"

    counts = collections.Counter(
        m.group(1)
        for p in self_path.glob("serde_arrow/src/test_impls/**/*.rs")
        for line in p.read_text(encoding="utf8").splitlines()
        if (m := re.match(pat, line)) is not None
    )

    print(
        "compilation support:",
        counts["true"],
        "/",
        counts["true"] + counts["false"],
        f"({counts['true'] / (counts['true'] + counts['false']):.0%})",
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
