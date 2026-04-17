#!/usr/bin/env python3
from __future__ import annotations

import argparse

from riposte_eval_figures import write_summary_table


def main() -> None:
    parser = argparse.ArgumentParser(description="Write a Markdown summary table for Riposte AWS eval results.")
    parser.add_argument("results_dir", help="Path to an aws-eval/results/<timestamp> directory")
    args = parser.parse_args()
    print(write_summary_table(args.results_dir))


if __name__ == "__main__":
    main()
