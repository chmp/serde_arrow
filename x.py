SELF_PATH = __import__("pathlib").Path(__file__).parent.resolve()

__effect = lambda effect: lambda func: [func, effect(func.__dict__)][0]
cmd = lambda **kw: __effect(lambda d: d.setdefault("@cmd", {}).update(kw))
arg = lambda *a, **kw: __effect(lambda d: d.setdefault("@arg", []).append((a, kw)))

ARROW_FEATURES = [
    # arrow-version:insert: "arrow-{version}",
    "arrow-59",
    "arrow-58",
    "arrow-57",
    "arrow-56",
    "arrow-55",
    "arrow-54",
    "arrow-53",
]


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
    _sh(
        f"""
            uv run python scripts/update-workflows.py
                --arrow-features {_q(ARROW_FEATURES)}
        """
    )


@cmd(help="Format the code")
def format():
    workflow("./.github/workflows/local.yml", "format")


@cmd(help="Run the linters")
def check():
    workflow("./.github/workflows/local.yml", "check")


@cmd()
@arg("path")
@arg("job", nargs="?")
def workflow(path, job):
    import json

    with open(path, "rt") as fobj:
        workflow = json.load(fobj)

    if not job:
        print(sorted(workflow["jobs"]))
        return

    for step in workflow["jobs"][job]["steps"]:
        if "run" not in step:
            print(step)
        else:
            _sh(step["run"], check=not step.get("continue-on-error", False))


@cmd(help="Run the serde_arrow_example")
@arg("--backtrace", action="store_true", default=False)
def serde_arrow_example(backtrace=False):
    workflow("./.github/workflows/local.yml", "example")


@cmd(help="Run both unit and integration tests")
@arg(
    "--backtrace",
    action="store_true",
    default=False,
    help="If given, print a backtrace on error",
)
def test(backtrace=False):
    workflow("./.github/workflows/local.yml", "test")


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
    _sh(
        f"""
            uv run python scripts/analyze-benchmark.py
                --criterion-root target/criterion
                --readme Readme.md
                --plot-output timings.png
                {"--update" if update else ""}
        """
    )


@cmd(help="Generate the documentation")
@arg("--private", action="store_true", default=False)
@arg("--open", action="store_true", default=False)
def doc(private=False, open=False):
    args = (("--document-private-items",) if private else ()) + (
        ("--open",) if open else ()
    )
    _sh(
        _cargo(
            "doc",
            *args,
            packages=("serde_arrow", "marrow"),
            features=(
                f"serde_arrow/{ARROW_FEATURES[0]}",
                "marrow/serde",
                f"marrow/{ARROW_FEATURES[0]}",
            ),
        ),
    )


@cmd(help="Add a new arrow version")
@arg("version")
def add_arrow_version(version):
    _sh(f"uv run python scripts/add-arrow-version.py{_q(version)}")
    format()
    update_workflows()


def _cargo(
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


_sh = lambda c, env=(), **kw: __import__("subprocess").run(
    [args := __import__("shlex").split(c.replace("\n", " ")), print("::", *args)][0],
    **{
        "check": True,
        "cwd": SELF_PATH,
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
