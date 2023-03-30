import argparse
import json
import os
import pathlib
import statistics
import subprocess
import sys
import toml

self_path = pathlib.Path(__file__).parent.resolve()

_md = lambda effect: lambda f: [f, effect(f)][0]
_ps = lambda o: vars(o).setdefault("__chmp__", {})
_as = lambda o: _ps(o).setdefault("__args__", [])
cmd = lambda **kw: _md(lambda f: _ps(f).update(kw))
arg = lambda *a, **k: _md(lambda f: _as(f).insert(0, (a, k)))


all_arrow_features = ["arrow-36"]

all_arrow2_features = ["arrow2-0-16", "arrow2-0-17"]

default_features = f"{all_arrow2_features[-1]},{all_arrow_features[-1]}"


@cmd()
@arg("--backtrace", action="store_true", default=False)
def precommit(backtrace=False):
    run(
        sys.executable,
        self_path / "serde_arrow" / "src" / "arrow2" / "gen_display_tests.py",
    )

    check_docs_config()

    cargo("fmt")

    for arrow2_feature in (*all_arrow2_features, *all_arrow_features):
        cargo("check", "--features", arrow2_feature)

    cargo("clippy", "--features", default_features)
    cargo(
        "test",
        "--features",
        default_features,
        env=dict(os.environ, RUST_BACKTRACE="1" if backtrace else "0"),
    )


@cmd()
def check_docs_config():
    with open(self_path / "serde_arrow" / "Cargo.toml", "rt") as fobj:
        config = toml.load(fobj)

    docs_features = sorted(config["package"]["metadata"]["docs"]["rs"]["features"])
    expected_features = sorted(default_features.split(","))

    if docs_features != expected_features:
        raise ValueError(
            "Invalid docs.rs configuration. "
            f"Expected features {expected_features}, found: {docs_features}"
        )


@cmd()
def test():
    for feature_flags in [
        ("--features", default_features),
        (),
    ]:
        cargo("test", *feature_flags, "--lib", env=dict(os.environ, RUST_BACKTRACE="1"))


@cmd()
def bench():
    cargo("bench", "--features", default_features)
    summarize_bench()


@cmd()
def summarize_bench():
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

    median_times = {k: statistics.median(v) for k, v in grouped_times.items()}

    print(f"{'':23s} serde_arrow   manual   ratio")
    for group in ["complex", "primitives"]:
        for op_label, serde_arrow_key, manual_key in [
            ("serialize", "serialize_into_arrays", "manually_serialize")
        ]:
            serde_arrow_time = median_times[group, serde_arrow_key]
            manual_time = median_times[group, manual_key]
            label = f"{op_label}({group})"
            print(
                f"{label:23s} "
                f"{1000 * serde_arrow_time:9.1f}ms "
                f"{1000 * manual_time:6.1f}ms "
                f"{serde_arrow_time / manual_time:6.1f}x"
            )

    print()
    print()

    print("# raw times")
    for (g, n), v in sorted(median_times.items()):
        label = f"{g}, {n}"
        print(f"{label:40s} {1000 * v:8.1f}ms")


def collect(kv_pairs):
    res = {}
    for k, v in kv_pairs:
        res.setdefault(k, []).append(v)

    return res


@cmd()
def doc():
    cargo("doc", "--features", default_arrow2_feature, cwd=self_path / "serde_arrow")


@cmd()
def release():
    cargo("package", "--list", cwd=self_path / "serde_arrow")

    if input("Continue [y/N]") == "y":
        cargo("publish", cwd=self_path / "serde_arrow")


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
