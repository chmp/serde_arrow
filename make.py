import argparse
import pathlib
import subprocess
import sys

from packaging import version

self_path = pathlib.Path(__file__).parent.resolve()

_md = lambda effect: lambda f: [f, effect(f)][0]
_ps = lambda o: vars(o).setdefault("__chmp__", {})
_as = lambda o: _ps(o).setdefault("__args__", [])
cmd = lambda **kw: _md(lambda f: _ps(f).update(kw))
arg = lambda *a, **k: _md(lambda f: _as(f).insert(0, (a, k)))


@cmd()
def precommit():
    cargo("fmt")
    cargo("clippy")
    cargo("test")


@cmd()
def doc():
    cargo("doc")


@cmd()
def release():
    # TODO: implement
    pass


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

    if "__main__" not in args:
        return parser.print_help()

    func = args.pop("__main__")
    return func(**args)


if __name__ == "__main__":
    main()
