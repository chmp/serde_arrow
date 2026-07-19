import argparse
import pathlib
import re
import subprocess


SELF_PATH = pathlib.Path(__file__).parents[1].resolve()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("version")
    add_arrow_version(parser.parse_args().version)


def add_arrow_version(version):
    if (
        subprocess.run(
            ["git", "diff-files", "--quiet"],
            check=False,
            cwd=SELF_PATH,
        ).returncode
        != 0
    ):
        print(
            "WARNING: potentially destructive changes. "
            "Please stage or commit the working tree first."
        )
        raise SystemExit(1)

    for path in arrow_version_paths():
        process_arrow_version_path(path, version)


def arrow_version_paths():
    return [
        SELF_PATH / "x.py",
        *SELF_PATH.glob("*.toml"),
        *SELF_PATH.glob("*/**/*.rs"),
        *SELF_PATH.glob("*/**/*.toml"),
    ]


def process_arrow_version_path(path, version):
    content = path.read_text()
    if "arrow-version" not in content:
        return

    print(f"process {path}")
    new_content = []
    include_next = True
    for line in content.splitlines():
        if (
            match := re.match(r"^.*(//|#) arrow-version:(replace|insert): (.*)$", line)
        ) is not None:
            new_content.append(line)
            new_content.append(
                match.group(3).format_map({"version": version, "\\n": "\n"})
            )
            include_next = match.group(2) != "replace"

        else:
            if include_next:
                new_content.append(line)

            include_next = True

    with open(path, "wt", newline="\n", encoding="utf-8") as fobj:
        fobj.write("\n".join(new_content))


if __name__ == "__main__":
    main()
