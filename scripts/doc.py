import argparse
import pathlib
import subprocess
import tomllib


SELF_PATH = pathlib.Path(__file__).parents[1].resolve()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--private", action="store_true", default=False)
    parser.add_argument("--open", action="store_true", default=False)
    doc(parser.parse_args())


def doc(args):
    extra_args = (("--document-private-items",) if args.private else ()) + (
        ("--open",) if args.open else ()
    )
    subprocess.run(
        split_command(
            cargo(
                "doc",
                *extra_args,
                packages=("serde_arrow", "marrow"),
                features=(
                    f"serde_arrow/{arrow_features()[0]}",
                    "marrow/serde",
                    f"marrow/{arrow_features()[0]}",
                ),
            )
        ),
        check=True,
        cwd=SELF_PATH,
    )


def arrow_features():
    return load_script_config()["arrow-features"]


def load_script_config():
    with open(SELF_PATH / "arrow-versions.toml", "rb") as fobj:
        return tomllib.load(fobj)


def cargo(command, *extra_args, packages=(), features=()):
    return " ".join(
        part
        for part in [
            "cargo",
            command,
            *(f"--package {package}" for package in packages),
            "--features" if features else "",
            ",".join(features) if features else "",
            *extra_args,
        ]
        if part
    )


def split_command(command):
    import shlex

    args = shlex.split(command.replace("\n", " "))
    print("::", *args)
    return args


if __name__ == "__main__":
    main()
