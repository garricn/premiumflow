#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A script to check file length."""
import argparse
import sys
from typing import Optional, Sequence


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Check file length."""
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs="*", help="Filenames to check.")
    parser.add_argument(
        "--max-lines",
        type=int,
        default=400,
        help="Maximum number of lines allowed in a file.",
    )
    args = parser.parse_args(argv)

    retv = 0

    for filename in args.filenames:
        try:
            with open(filename, "r") as f:
                lines = f.readlines()
                has_ignore_comment = any("# file-length-ignore" in line for line in lines[:5])

                if len(lines) > args.max_lines and not has_ignore_comment:
                    print(
                        f"{filename}: file is too long " f"({len(lines)} > {args.max_lines} lines)"
                    )
                    retv = 1
        except Exception as e:
            print(f"Error checking {filename}: {e}")
            retv = 1

    return retv


if __name__ == "__main__":
    sys.exit(main())
