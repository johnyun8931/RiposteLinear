#!/usr/bin/env python3
from __future__ import annotations

import argparse

from riposte_eval_figures import plot_aes_reference


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot idle OpenSSL AES-128-CTR reference throughput.")
    parser.add_argument("results_dir", help="Path to an aws-eval/results/<timestamp> directory")
    args = parser.parse_args()
    print(plot_aes_reference(args.results_dir))


if __name__ == "__main__":
    main()
