SELF_PATH = __import__("pathlib").Path(__file__).parent.resolve()

__effect = lambda effect: lambda func: [func, effect(func.__dict__)][0]
cmd = lambda **kw: __effect(lambda d: d.setdefault("@cmd", {}).update(kw))
arg = lambda *a, **kw: __effect(lambda d: d.setdefault("@arg", []).append((a, kw)))


backtrace = lambda name: arg(
    name,
    action="store_true",
    default=False,
    help="If given, print a backtrace on error",
)


@cmd(help="Run all common development tasks before a commit")
@backtrace("--backtrace")
def precommit(backtrace=False):
    update_workflows()

    format()
    check()
    test(backtrace=backtrace)
    serde_arrow_example(backtrace)


@cmd(help="Update the github workflows")
def update_workflows():
    _sh("uv run python scripts/update-workflows.py")


@cmd(help="Format the code")
def format():
    workflow(SELF_PATH / ".github/workflows/local.yml", "format")


@cmd(help="Run the linters")
def check():
    workflow(SELF_PATH / ".github/workflows/local.yml", "check")


@cmd(help="Run the serde_arrow_example")
@backtrace("--backtrace")
def serde_arrow_example(backtrace=False):
    workflow(
        SELF_PATH / ".github/workflows/local.yml",
        "example",
        env=({"RUST_BACKTRACE": "1"} if backtrace else {}),
    )


@cmd(help="Run the full CI test flow locally")
@backtrace("--backtrace")
def ci_test(backtrace=False):
    workflow(
        SELF_PATH / ".github/workflows/test.yml",
        "test",
        env=({"RUST_BACKTRACE": "1"} if backtrace else {}),
    )


@cmd(help="Run both unit and integration tests")
@backtrace("--backtrace")
def test(backtrace=False):
    workflow(
        SELF_PATH / ".github/workflows/local.yml",
        "test",
        env=({"RUST_BACKTRACE": "1"} if backtrace else {}),
    )


@cmd(help="Run the benchmarks")
@arg("--quick", action="store_true", default=False)
def serde_arrow_bench(quick=False):
    workflow(
        SELF_PATH / ".github/workflows/bench.yml",
        "bench",
        env=({"SERDE_ARROW_BENCH_QUICK": "1"} if quick else {}),
    )


@cmd(help="Summarize existing benchmarks")
@arg("--update", action="store_true", default=False)
def summarize_bench(update=False):
    _sh(
        f"""
            uv run python scripts/analyze-benchmark.py
                --criterion-root target/criterion
                {"--update Readme.md" if update else ""}
        """
    )


@cmd(help="Generate the documentation")
@arg("--private", action="store_true", default=False)
@arg("--open", action="store_true", default=False)
def doc(private=False, open=False):
    _sh(
        f"""
            uv run python scripts/doc.py
                {"--private" if private else ""}
                {"--open" if open else ""}
        """
    )


@cmd(help="Add a new arrow version")
@arg("version")
def add_arrow_version(version):
    import shlex

    _sh(f"uv run python scripts/add-arrow-version.py {shlex.quote(version)}")
    format()
    update_workflows()


def workflow(path, job, *, env=()):
    import json

    with open(path, "rt") as fobj:
        workflow = json.load(fobj)

    if not job:
        print(sorted(workflow["jobs"]))
        return

    for step in workflow["jobs"][job]["steps"]:
        if "run" in step:
            _sh(step["run"], env=env)


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

if __name__ == "__main__":
    _sps = (_p := __import__("argparse").ArgumentParser()).add_subparsers()
    for _f in (f for _, f in sorted(globals().items()) if hasattr(f, "@cmd")):
        _kw = {"name": _f.__name__.replace("_", "-"), **getattr(_f, "@cmd")}
        (_sp := _sps.add_parser(**_kw)).set_defaults(_=_f)
        [_sp.add_argument(*a, **kw) for a, kw in reversed(getattr(_f, "@arg", []))]
    (_a := vars(_p.parse_args())).pop("_", _p.print_help)(**_a)
