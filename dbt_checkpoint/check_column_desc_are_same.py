import argparse
import os
import time
from collections import Counter
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Any, Dict, Generator, Iterator, Optional, Sequence, Tuple

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    ModelSchema,
    add_default_args,
    get_dbt_manifest,
    get_filenames,
    get_model_schemas,
    red,
    yellow,
)


@dataclass
class ColumnDescription:
    column_name: str
    description: str
    file: Path
    new_description: Optional[str] = None  # pragma: no mutate


def get_all_columns(
    schemas: Generator[ModelSchema, None, None], ignore_list: Sequence[str]
) -> Generator[ColumnDescription, None, None]:
    for item in schemas:
        for column in item.schema.get("columns", {}):
            desc = column.get("description")
            column_name = column.get("name")
            if column_name not in ignore_list:
                yield ColumnDescription(column_name, desc, item.file)


def get_grouped(
    paths: Sequence[str], ignore: Optional[Sequence[str]]
) -> Iterator[Tuple[str, Iterator[ColumnDescription]]]:
    ignore_list = ignore or []
    ymls = get_filenames(paths, [".yml", ".yaml"])
    filenames = set(ymls.keys())

    schemas = get_model_schemas(list(ymls.values()), filenames, True)
    columns = list(get_all_columns(schemas, ignore_list)) # Convert to list to print it

    print(f"\n--- RAW DATA BEFORE SORTING ({len(columns)} columns) ---")
    for c in columns:
        print(f"FILE: {c.file.name} | COL: '{c.column_name}'")

    # The sorting happens here - let's see if it works
    sorted_cols = sorted(columns, key=lambda x: x.column_name)
    
    print(f"\n--- DATA AFTER SORTING ---")
    for c in sorted_cols:
        print(f"COL: '{c.column_name}'")

    grouped = groupby(sorted_cols, lambda x: x.column_name)
    return grouped


def check_column_desc(paths: Sequence[str], ignore: Optional[Sequence[str]]) -> Dict[str, Any]:
    status_code = 0
    all_columns = []
    ignore_list = ignore or []

    print(f"\n=== EXECUTION START ===")
    print(f"Paths received by script: {paths}")

    # 1. Aggregate ALL columns from ALL paths
    ymls = get_filenames(paths, [".yml", ".yaml"])
    filenames = set(ymls.keys())
    schemas = get_model_schemas(list(ymls.values()), filenames, True)
    
    for col in get_all_columns(schemas, ignore_list):
        all_columns.append(col)

    print(f"Total columns found across all files: {len(all_columns)}")
    for c in all_columns:
        print(f"  - Column '{c.column_name}' found in {c.file.name}")

    # 2. Sort the master list by column name
    all_columns.sort(key=lambda x: x.column_name)

    # 3. Group and Compare
    grouped = groupby(all_columns, lambda x: x.column_name)

    for name, groups in grouped:
        group_list = list(groups)
        # We only care if the column name appears in more than 1 place
        if len(group_list) > 1:
            group_cnt = Counter([g.description for g in group_list])
            
            print(f"CHECKING: '{name}' (found {len(group_list)} instances)")
            
            if len(group_cnt.keys()) > 1:
                status_code = 1
                print(f"  [!] CONFLICT FOUND for '{red(name)}'")
                for desc, count in group_cnt.items():
                    print(f"      - {yellow(count)} instance(s) with desc: {yellow(desc)}")
        else:
            # Optional: print for single instances
            # print(f"SKIPPING: '{name}' (only found in 1 place)")
            pass

    print(f"=== EXECUTION END (Status: {status_code}) ===\n")
    return {"status_code": status_code}


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--ignore",
        nargs="*",
        help="Columns for which do not check whether have a different description.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = check_column_desc(paths=args.filenames, ignore=args.ignore)
    end_time = time.time()

    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check column descriptions are the same.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
