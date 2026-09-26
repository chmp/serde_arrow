import argparse
import json
import os
import pathlib
import statistics

SELF_PATH = pathlib.Path(__file__).parents[1].resolve()

BENCHMARK_RENAMES = {
    "arrow": "arrow_json::ReaderBuilder",
    "arrow_builder": "arrow builder",
    "serde_arrow_arrow": "serde_arrow::to_arrow",
    "serde_arrow_marrow": "serde_arrow::to_marrow",
    "serde_arrow_marrow_push": "serde_arrow::ArrayBuilder::push",
    "serde_arrow_marrow_to_arrow": "serde_arrow::to_marrow + Arrow conversion",
}
DESERIALIZATION_RENAMES = {
    "arrow_manual": "manual",
    "serde_arrow_arrow": "serde_arrow::from_arrow",
    "serde_arrow_marrow": "serde_arrow::from_marrow",
    "serde_arrow_marrow_iter": "Deserializer::iter",
}
BENCHMARK_BASELINE = "arrow builder"
DESERIALIZATION_BASELINE = "manual"
README_BENCHMARK_IGNORE_GROUPS = {
    "binary_values_1000",
    "binary_values_1000_deserialize",
    "json_to_arrow",
    "wide_schema_1024",
}
README_BENCHMARK_IGNORE_IMPLS = {
    "serde_arrow::ArrayBuilder::push",
    "serde_arrow::to_marrow + Arrow conversion",
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--criterion-root", type=pathlib.Path, required=True)
    parser.add_argument(
        "--plot-output", type=pathlib.Path, default=pathlib.Path("timings.png")
    )
    parser.add_argument(
        "--plot",
        action="store_true",
        help="Generate the benchmark timing chart.",
    )
    parser.add_argument("--update", type=pathlib.Path)
    parser.add_argument("--update-github-summary", action="store_true", default=False)
    analyze_benchmark(parser.parse_args())


def analyze_benchmark(args):
    root = resolve_path(args.criterion_root)
    update = resolve_path(args.update) if args.update else None
    plot_output = resolve_path(args.plot_output)

    mean_times = {
        key: time
        for key, time in load_times(root).items()
        if key[1] not in README_BENCHMARK_IGNORE_IMPLS
    }
    benchmark = format_benchmark(
        mean_times,
        ignore_groups=README_BENCHMARK_IGNORE_GROUPS,
    )

    print(benchmark)

    if update is not None:
        update_marked_output(update, benchmark)

    if args.update_github_summary:
        update_github_summary(benchmark)

    if args.plot:
        plot_times(
            mean_times,
            benchmark_baseline=BENCHMARK_BASELINE,
            ignore_groups=README_BENCHMARK_IGNORE_GROUPS,
            output=plot_output,
        )


def resolve_path(path):
    if path.is_absolute():
        return path

    return SELF_PATH / path


def load_times(root):
    results = []
    for p in root.glob("*/*/new/sample.json"):
        group = p.parent.parent.parent.name
        name = p.parent.parent.name
        with open(p) as fobj:
            data = json.load(fobj)

        for iterations, time in zip(data["iters"], data["times"]):
            results.append(
                {
                    "name": (
                        DESERIALIZATION_RENAMES.get(name, name)
                        if group.endswith("_deserialize")
                        else BENCHMARK_RENAMES.get(name, name)
                    ),
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


def collect(kv_pairs):
    res = {}
    for k, v in kv_pairs:
        res.setdefault(k, []).append(v)

    return res


def format_benchmark(mean_times, ignore_groups=()):
    def _parts():
        groups = {g for g, _ in mean_times if g not in ignore_groups}
        for title, selected in (
            (
                "Serialization",
                sorted(g for g in groups if not g.endswith("_deserialize")),
            ),
            (
                "Deserialization",
                sorted(g for g in groups if g.endswith("_deserialize")),
            ),
        ):
            if not selected:
                continue
            yield f"### {title}"
            yield ""
            for group in selected:
                yield from _format_group(group)

    def _format_group(group):
        times_in_group = {n: v for (g, n), v in mean_times.items() if g == group}
        sorted_items = sorted(times_in_group.items(), key=lambda kv: kv[1])
        rows = [["label", "time [ms]", *(name[:15] for name, _ in sorted_items)]]
        for label, time in sorted_items:
            rows.append(
                [
                    label,
                    f"{1000 * time:7.2f}",
                    *(f"{time / comparison:.2f}" for _, comparison in sorted_items),
                ]
            )

        widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]))]

        yield f"#### `{group.removesuffix('_deserialize')}`"
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


def update_marked_output(output, content):
    print(f"Update markers in {output}")
    with open(output, "rt", encoding="utf8") as fobj:
        lines = [line.rstrip() for line in fobj]

    with open(output, "wt", encoding="utf8", newline="\n") as fobj:
        for line in replace_marked_section(
            lines,
            start_marker="<!-- start:benchmarks -->",
            end_marker="<!-- end:benchmarks -->",
            content=content,
        ):
            print(line, file=fobj)


def update_github_summary(content):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path is None:
        return

    append_output(pathlib.Path(path), content)


def append_output(path, content):
    print(f"Append summary to {path}")
    with open(path, "at", encoding="utf8", newline="\n") as fobj:
        print(content, file=fobj)


def replace_marked_section(lines, *, start_marker, end_marker, content):
    start = None
    end = None
    for idx, line in enumerate(lines):
        if line.strip() == start_marker:
            start = idx
        elif line.strip() == end_marker:
            end = idx
            break

    if start is None or end is None or end < start:
        raise RuntimeError(
            f"Could not find marker block {start_marker!r}..{end_marker!r}"
        )

    return [
        *lines[: start + 1],
        *content.splitlines(),
        *lines[end:],
    ]


def plot_times(mean_times, *, benchmark_baseline, ignore_groups, output):
    print("Plot times")

    import matplotlib.pyplot as plt

    def relative_times(baseline, *, deserialize):
        groups = {
            group
            for group, impl in mean_times
            if impl == baseline
            and group not in ignore_groups
            and group.endswith("_deserialize") == deserialize
        }
        ratios = collect(
            (impl, time / mean_times[group, baseline])
            for (group, impl), time in mean_times.items()
            if group in groups
        )
        return sorted(
            ((impl, statistics.mean(values)) for impl, values in ratios.items()),
            key=lambda item: item[1],
        )

    fig, axes = plt.subplots(1, 2, figsize=(14, 4), dpi=150)
    for ax, title, baseline, deserialize in (
        (axes[0], "Serialization", benchmark_baseline, False),
        (axes[1], "Deserialization", DESERIALIZATION_BASELINE, True),
    ):
        values = relative_times(baseline, deserialize=deserialize)
        bars = ax.barh(
            [impl for impl, _ in values],
            [ratio for _, ratio in values],
            zorder=10,
        )
        ax.bar_label(
            bars,
            [f"{ratio:.1f} x" for _, ratio in values],
            bbox={"boxstyle": "square,pad=0.0", "fc": "white", "ec": "none"},
            padding=2.5,
        )
        ax.grid(axis="x", zorder=0)
        ax.set_xlim(0, 1.15 * max(ratio for _, ratio in values))
        ax.set_title(title)
        ax.set_xlabel(f"Mean runtime relative to {baseline}")

    fig.tight_layout(w_pad=3)
    fig.savefig(output)


if __name__ == "__main__":
    main()
