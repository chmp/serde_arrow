import pathlib
import re


SELF_PATH = pathlib.Path(__file__).parents[1].resolve()
SERDE_ARROW_SRC = SELF_PATH / "serde_arrow" / "src"
ARROW_IMPL = SERDE_ARROW_SRC / "arrow_impl.rs"
CFG_FEATURE_RE = re.compile(r"#\s*\[\s*cfg\b[^\]]*\bunsafe-optimizations\b[^\]]*\]")


def main():
    print(":: check unsafe-optimizations cfg dispatch")
    dispatch_count = 0
    violations = []

    for path in SERDE_ARROW_SRC.rglob("*.rs"):
        source = path.read_text(encoding="utf8")
        relative_path = path.relative_to(SELF_PATH)

        for match in CFG_FEATURE_RE.finditer(source):
            if path == ARROW_IMPL:
                dispatch_count += 1
                continue

            line = source.count("\n", 0, match.start()) + 1
            violations.append(f"{relative_path}:{line}")

    if dispatch_count == 0:
        raise ValueError("No unsafe-optimizations cfg dispatch found in arrow_impl.rs")

    if violations:
        locations = ", ".join(violations)
        raise ValueError(
            "unsafe-optimizations cfg dispatch found outside arrow_impl.rs: "
            f"{locations}"
        )


if __name__ == "__main__":
    main()
