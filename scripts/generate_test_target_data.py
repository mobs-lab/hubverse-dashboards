"""
Generate Fake Target Data for Testing Data-Update Runs

Produces a hubverse-compatible target-data parquet or CSV file with
controllable size, date grid, and as_of snapshots. Designed so you can:

1. Run once -> from-scratch build
2. Re-run with more weeks or a different seed -> data-update build
3. Compare the evaluation computation summary between the two

Usage:
    python scripts/generate_test_target_data.py [options]

Examples:
    # Default: 20 weeks, 4 locations, 2 targets, 3 as_of snapshots -> parquet
    python scripts/generate_test_target_data.py

    # Extend to 24 weeks (simulates a target-data update with 4 new weeks)
    python scripts/generate_test_target_data.py --num-weeks 24

    # Single-file without as_of column (tests the no-as_of code path)
    python scripts/generate_test_target_data.py --no-as-of

    # CSV format instead of parquet
    python scripts/generate_test_target_data.py --format csv

    # Custom output directory
    python scripts/generate_test_target_data.py --output-dir development-mode-root/target-data
"""

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def generate_fake_target_data(
    start_date: str = "2024-09-07",
    num_weeks: int = 5,
    locations: list = None,
    targets: dict = None,
    num_as_of_snapshots: int = 3,
    include_as_of: bool = True,
    seed: int = 42,
    output_format: str = "csv",
    output_dir: str = None,
    file_name: str = "time-series",
) -> pd.DataFrame:
    """
    Generate fake target data adhering to the hubverse time-series format.

    The date grid always lands on the same day of the week as ``start_date``.
    With the default start of 2024-09-07 (a Saturday), all dates will be
    Saturdays, matching the CDC NHSN weekly reporting convention.

    Args:
        start_date: First date in the series.  Must fall on the desired day
            of the week (default: Saturday 2024-09-07).
        num_weeks: Number of weekly observation dates to generate.
        locations: List of location codes (e.g. ``["US", "06", "25"]``).
        targets: Dict mapping target name -> ``(mean, std)`` for generating
            random observations.  Defaults to the two standard CovidHub
            targets.
        num_as_of_snapshots: Number of as_of snapshots.  The last snapshot
            covers all weeks and represents the current ground truth.
        include_as_of: If False, the ``as_of`` column is omitted entirely.
            This tests the single-file-no-as_of code path.
        seed: Random seed for reproducibility.
        output_format: ``"parquet"`` or ``"csv"``.
        output_dir: Directory to write into.  Resolved relative to the
            project root (parent of ``scripts/``).
        file_name: Stem of the output file (without extension).

    Returns:
        The generated DataFrame.
    """
    rng = np.random.default_rng(seed)

    if locations is None:
        locations = ["US", "01" ,"06", "25", "36"]

    if targets is None:
        targets = {
            "wk inc covid hosp": {"mean": 500.0, "std": 150.0, "decimals": 0},
            "wk inc covid prop ed visits": {"mean": 0.02, "std": 0.005, "decimals": 6},
        }

    # Validate start_date day-of-week
    base_date = pd.Timestamp(start_date)
    day_name = base_date.day_name()
    print(f"Start date: {base_date.date()} ({day_name})")
    print(f"All {num_weeks} dates will land on {day_name}s")

    # Generate the weekly date grid (freq=7D from the start ensures same DOW)
    dates = pd.date_range(start=base_date, periods=num_weeks, freq="7D")

    # Determine as_of schedule
    if include_as_of and num_as_of_snapshots > 0:
        weeks_per_snapshot = max(1, num_weeks // num_as_of_snapshots)
        snapshot_cutoffs = []
        for snap_idx in range(num_as_of_snapshots):
            weeks_visible = min((snap_idx + 1) * weeks_per_snapshot, num_weeks)
            snapshot_cutoffs.append(weeks_visible)
        # Ensure the last snapshot covers everything
        snapshot_cutoffs[-1] = num_weeks
    else:
        include_as_of = False
        snapshot_cutoffs = [num_weeks]

    all_rows = []

    for snap_idx, weeks_visible in enumerate(snapshot_cutoffs):
        visible_dates = dates[:weeks_visible]

        # as_of is the Wednesday after the last visible Saturday (+4 days)
        # This matches the CovidHub convention where data is released on Wednesdays
        as_of_date = visible_dates[-1] + pd.Timedelta(days=4)

        for target_name, target_params in targets.items():
            mean = target_params["mean"]
            std = target_params["std"]
            decimals = target_params.get("decimals", 2)

            for loc in locations:
                for d in visible_dates:
                    obs = max(0, rng.normal(mean, std))
                    obs = round(obs, decimals) if decimals > 0 else int(round(obs))

                    row = {
                        "date": d,
                        "location": loc,
                        "observation": obs,
                        "target": target_name,
                    }
                    if include_as_of:
                        row["as_of"] = as_of_date
                    all_rows.append(row)

    df = pd.DataFrame(all_rows)

    # Enforce column order matching the hubverse convention
    col_order = ["date", "location", "observation"]
    if include_as_of:
        col_order.append("as_of")
    col_order.append("target")
    df = df[col_order]

    # Resolve output path
    project_root = Path(__file__).parent.parent
    if output_dir:
        out_dir = project_root / output_dir
    else:
        out_dir = project_root / "development-mode-root" / "target-data"
    out_dir.mkdir(parents=True, exist_ok=True)

    if output_format == "csv":
        out_path = out_dir / f"{file_name}.csv"
        df.to_csv(out_path, index=False)
    else:
        out_path = out_dir / f"{file_name}.parquet"
        df.to_parquet(out_path, index=False)

    # Print summary
    print()
    print("=" * 60)
    print("GENERATED TEST TARGET DATA")
    print("=" * 60)
    print(f"  Output:       {out_path}")
    print(f"  Format:       {output_format}")
    print(f"  Total rows:   {len(df):,}")
    print(f"  Date range:   {dates[0].date()} to {dates[-1].date()} ({num_weeks} {day_name}s)")
    print(f"  Locations:    {locations}")
    print(f"  Targets:      {list(targets.keys())}")
    if include_as_of:
        print(f"  as_of snaps:  {num_as_of_snapshots} (latest covers all {num_weeks} weeks)")
    else:
        print("  as_of column: OMITTED (single-file no-as_of mode)")
    print(f"  Random seed:  {seed}")
    print("=" * 60)
    print()
    print("To simulate a data-update, re-run with --num-weeks or --seed changed.")
    print()

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Generate fake hubverse target-data for testing data-update runs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default generation (20 weeks, parquet, with as_of)
  python scripts/generate_test_target_data.py

  # Simulate an update: add 4 more weeks of data
  python scripts/generate_test_target_data.py --num-weeks 24

  # Test single-file without as_of column
  python scripts/generate_test_target_data.py --no-as-of

  # CSV format
  python scripts/generate_test_target_data.py --format csv --file-name my-test-data
""",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default="2024-09-07",
        help="First date in the series (must be the desired day-of-week). Default: 2024-09-07 (Saturday)",
    )
    parser.add_argument(
        "--num-weeks",
        type=int,
        default=5,
        help="Number of weekly observations to generate. Default: 20",
    )
    parser.add_argument(
        "--locations",
        nargs="+",
        default=None,
        help='Location codes. Default: US 06 25 36',
    )
    parser.add_argument(
        "--num-snapshots",
        type=int,
        default=3,
        help="Number of as_of snapshots. Default: 3",
    )
    parser.add_argument(
        "--no-as-of",
        action="store_true",
        help="Omit the as_of column (tests single-file no-as_of code path)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility. Default: 42",
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["parquet", "csv"],
        default="parquet",
        help="Output file format. Default: parquet",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (relative to project root). Default: development-mode-root/target-data",
    )
    parser.add_argument(
        "--file-name",
        type=str,
        default="time-series",
        help='Output file stem (without extension). Default: "time-series"',
    )

    args = parser.parse_args()

    generate_fake_target_data(
        start_date=args.start_date,
        num_weeks=args.num_weeks,
        locations=args.locations,
        num_as_of_snapshots=args.num_snapshots,
        include_as_of=not args.no_as_of,
        seed=args.seed,
        output_format=args.format,
        output_dir=args.output_dir,
        file_name=args.file_name,
    )


if __name__ == "__main__":
    main()
