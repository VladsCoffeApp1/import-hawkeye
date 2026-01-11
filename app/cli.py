#!/usr/bin/env python3
# /// script
# requires-python = "==3.12.9"
# dependencies = []
# ///

"""
SPDX-License-Identifier: LicenseRef-NonCommercial-Only
2025 github.com/defmon3 - Non-commercial use only. Commercial use requires permission.
Format docstrings according to PEP 287
File: cli.py

CLI tool for running the import-hawkeye pipeline locally with real data.

Usage:
    python -m app.cli path/to/file.zip
    python -m app.cli path/to/file.zip --show-data
    python -m app.cli path/to/file.zip --show-data --rows 20
    python -m app.cli path/to/file.zip --output results.csv
"""

import argparse
import sys
from pathlib import Path

import pandas as pd
from loguru import logger as log

from . import conductor
from .conductor import PipelineResult, ProcessedCSV


def configure_logging(verbose: bool = False) -> None:
    """
    Configure loguru for CLI output.

    :param verbose: If True, show DEBUG level logs
    """
    log.remove()
    level = "DEBUG" if verbose else "INFO"
    log.add(sys.stderr, format="<level>{level: <8}</level> | {message}", level=level, colorize=True)


def print_summary(result: PipelineResult) -> None:
    """
    Print a summary of the pipeline results.

    :param result: PipelineResult from conductor.run()
    """
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)

    print(f"\nTotal rows processed: {result.total_rows}")
    print(f"Files processed: {len(result.processed)}")
    print(f"  Successful: {len(result.successful)}")
    print(f"  Failed: {len(result.failed)}")

    if result.successful:
        print("\n--- Successful Files ---")
        for p in result.successful:
            print(f"  {p.filename}")
            print(f"    Data type: {p.data_type.value if p.data_type else 'Unknown'}")
            print(f"    Target table: {p.table}")
            print(f"    Rows: {p.rows_loaded}")

    if result.failed:
        print("\n--- Failed Files ---")
        for p in result.failed:
            print(f"  {p.filename}: {p.error}")


def print_dataframe_info(processed: ProcessedCSV, max_rows: int = 10) -> None:
    """
    Print DataFrame contents for a processed CSV.

    :param processed: ProcessedCSV with DataFrame
    :param max_rows: Maximum number of rows to display
    """
    if processed.df is None:
        return

    df = processed.df
    print(f"\n--- DataFrame: {processed.filename} ---")
    print(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"Columns: {list(df.columns)}")
    print(f"\nData types:")
    for col, dtype in df.dtypes.items():
        print(f"  {col}: {dtype}")

    print(f"\nFirst {min(max_rows, len(df))} rows:")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", 50)
    print(df.head(max_rows).to_string(index=False))


def print_bigquery_info(result: PipelineResult) -> None:
    """
    Print what would be written to BigQuery.

    :param result: PipelineResult from conductor.run()
    """
    print("\n" + "=" * 60)
    print("BIGQUERY OPERATIONS (DRY RUN)")
    print("=" * 60)

    for p in result.successful:
        print(f"\n  Table: {p.table}")
        print(f"  Rows to insert: {p.rows_loaded}")
        if p.df is not None:
            print(f"  Columns: {list(p.df.columns)}")


def export_to_csv(result: PipelineResult, output_path: Path) -> None:
    """
    Export all processed DataFrames to a CSV file.

    :param result: PipelineResult from conductor.run()
    :param output_path: Path to output CSV file
    """
    all_dfs = []

    for p in result.successful:
        if p.df is not None:
            df = p.df.copy()
            df["_source_file"] = p.filename
            df["_data_type"] = p.data_type.value if p.data_type else None
            df["_target_table"] = p.table
            all_dfs.append(df)

    if all_dfs:
        combined = pd.concat(all_dfs, ignore_index=True)
        combined.to_csv(output_path, index=False)
        print(f"\nExported {len(combined)} rows to {output_path}")
    else:
        print("\nNo data to export")


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.

    :returns: Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Run the import-hawkeye pipeline locally with real data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m app.cli data.zip                     # Process single ZIP file
  python -m app.cli data/                        # Process all ZIPs in directory
  python -m app.cli data.zip -d                  # Show DataFrame contents
  python -m app.cli data/ -d --rows 20           # Process directory with options
  python -m app.cli data.zip --output out.csv    # Export to CSV (single file only)
  python -m app.cli data.zip -v                  # Verbose logging
        """,
    )

    parser.add_argument(
        "path",
        type=Path,
        help="Path to ZIP file or directory containing ZIP files",
    )

    parser.add_argument(
        "-d",
        "--show-data",
        action="store_true",
        help="Show actual DataFrame rows (default: just summary)",
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=10,
        metavar="N",
        help="Limit rows shown when using --show-data (default: 10)",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        metavar="FILE",
        help="Export combined results to CSV file",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )

    parser.add_argument(
        "--bq-no-dry",
        action="store_true",
        help="Actually write to BigQuery (default: dry-run only)",
    )

    return parser.parse_args()


def process_single_file(
    zip_path: Path, show_data: bool = False, max_rows: int = 10, output_path: Path | None = None
) -> int:
    """
    Process a single ZIP file.

    :param zip_path: Path to ZIP file
    :param show_data: Show DataFrame contents
    :param max_rows: Maximum rows to display
    :param output_path: Optional path to export CSV
    :returns: Exit code (0 for success, 1 for error, 2 for partial success)
    """
    # Read ZIP file
    log.info(f"Reading ZIP file: {zip_path}")
    zip_data = zip_path.read_bytes()
    log.info(f"ZIP file size: {len(zip_data):,} bytes")

    # Run pipeline in dry-run mode (NEVER writes to BigQuery)
    try:
        result = conductor.run(zip_data, dry_run=True)
    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        return 1

    # Print summary
    print_summary(result)

    # Print BigQuery operations
    print_bigquery_info(result)

    # Show DataFrame contents if requested
    if show_data:
        for p in result.successful:
            print_dataframe_info(p, max_rows)

    # Export to CSV if requested
    if output_path:
        export_to_csv(result, output_path)

    # Return exit code based on results
    if not result.successful:
        return 1
    if result.failed:
        return 2  # Partial success

    return 0


def process_directory(directory: Path, show_data: bool = False, max_rows: int = 10) -> int:
    """
    Process all ZIP files in a directory.

    :param directory: Path to directory
    :param show_data: Show DataFrame contents
    :param max_rows: Maximum rows to display
    :returns: Exit code (0 for success, 1 for error)
    """
    # Find all ZIP files
    zip_files = sorted(directory.glob("*.zip"))

    if not zip_files:
        log.warning(f"No ZIP files found in directory: {directory}")
        return 0

    log.info(f"Found {len(zip_files)} ZIP file(s) in {directory}")
    print()

    successful = 0
    failed = 0
    partial = 0

    for zip_file in zip_files:
        print("=" * 80)
        print(f"Processing: {zip_file.name}")
        print("=" * 80)
        print()

        exit_code = process_single_file(zip_file, show_data, max_rows)

        if exit_code == 0:
            successful += 1
            log.info(f"Success: {zip_file.name}")
        elif exit_code == 2:
            partial += 1
            log.warning(f"Partial success: {zip_file.name}")
        else:
            failed += 1
            log.error(f"Failed: {zip_file.name}")

        print()

    # Print summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nTotal files: {len(zip_files)}")
    print(f"Successful: {successful}")
    print(f"Partial success: {partial}")
    print(f"Failed: {failed}")

    # Return exit code
    if failed > 0:
        return 1
    return 0


def main() -> int:
    """
    CLI entry point.

    :returns: Exit code (0 for success, 1 for error)
    """
    args = parse_args()
    configure_logging(args.verbose)

    # Validate path exists
    if not args.path.exists():
        log.error(f"Path not found: {args.path}")
        return 1

    # Process directory or file
    if args.path.is_dir():
        if args.output:
            log.error("--output flag not supported for directory processing")
            return 1
        return process_directory(args.path, args.show_data, args.rows)
    elif args.path.is_file():
        if not args.path.suffix.lower() == ".zip":
            log.error(f"File must be a ZIP file: {args.path}")
            return 1
        return process_single_file(args.path, args.show_data, args.rows, args.output)
    else:
        log.error(f"Path is neither a file nor a directory: {args.path}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
