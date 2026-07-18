import argparse
import json
import pathlib
import statistics


SELF_PATH = pathlib.Path(__file__).parents[1].resolve()

BENCHMARK_RENAMES = {
    "arrow": "arrow_json::ReaderBuilder",
    "arrow_builder": "arrow builder",
    "serde_arrow_arrow": "serde_arrow::to_arrow",
    "serde_arrow_marrow": "serde_arrow::to_marrow",
}
BENCHMARK_BASELINE = "arrow builder"
README_BENCHMARK_IGNORE_GROUPS = {"json_to_arrow"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--criterion-root", type=pathlib.Path, required=True)
    parser.add_argument("--plot-output", type=pathlib.Path, required=True)
    parser.add_argument("--readme", type=pathlib.Path, required=True)
    parser.add_argument("--update", action="store_true", default=False)
    analyze_benchmark(parser.parse_args())


def analyze_benchmark(args):
    root = resolve_path(args.criterion_root)
    readme = resolve_path(args.readme)
    plot_output = resolve_path(args.plot_output)

    mean_times = load_times(root)
    print(format_benchmark(mean_times, ignore_groups=README_BENCHMARK_IGNORE_GROUPS))

    if args.update:
        update_readme(
            readme,
            mean_times,
            ignore_groups=README_BENCHMARK_IGNORE_GROUPS,
        )
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
                    "name": BENCHMARK_RENAMES.get(name, name),
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
        for group in sorted({g for g, _ in mean_times if g not in ignore_groups}):
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

            yield f"### `{group}`"
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


def update_readme(readme, mean_times, ignore_groups=()):
    print("Update readme")
    with open(readme, "rt", encoding="utf8") as fobj:
        lines = [line.rstrip() for line in fobj]

    with open(readme, "wt", encoding="utf8", newline="\n") as fobj:
        for line in replace_marked_section(
            lines,
            start_marker="<!-- start:benchmarks -->",
            end_marker="<!-- end:benchmarks -->",
            content=format_benchmark(mean_times, ignore_groups=ignore_groups),
        ):
            print(line, file=fobj)


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
    import polars as pl

    df = pl.from_dicts(
        [
            {"group": group, "impl": impl, "time": time}
            for (group, impl), time in mean_times.items()
            if group not in ignore_groups
        ]
    )
    agg_df = (
        df.select(
            [
                pl.col("impl"),
                (
                    pl.col("time")
                    / pl.col("time")
                    .filter(pl.col("impl") == benchmark_baseline)
                    .mean()
                    .over("group")
                ),
            ]
        )
        .group_by("impl")
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
    plt.subplots_adjust(left=0.32, right=0.975, top=0.95, bottom=0.15)
    plt.xlabel(f"Mean runtime compared to {benchmark_baseline}")
    plt.savefig(output)


if __name__ == "__main__":
    main()
