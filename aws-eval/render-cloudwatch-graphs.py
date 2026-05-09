#!/usr/bin/env python3

import argparse
import base64
import json
import pathlib
import re
import shlex
import subprocess


def parse_state(path: pathlib.Path) -> dict[str, str]:
    state: dict[str, str] = {}
    for line in path.read_text().splitlines():
        match = re.match(r"^([^=]+)=(.*)$", line)
        if not match:
            continue
        key, raw_value = match.group(1), match.group(2).strip()
        try:
            parts = shlex.split(raw_value)
            value = parts[0] if parts else ""
        except ValueError:
            value = raw_value.strip("'\"")
        state[key] = value
    return state


def alb_dimension(arn: str) -> str:
    return arn.split("loadbalancer/", 1)[1]


def target_group_dimension(arn: str) -> str:
    return "targetgroup/" + arn.split("targetgroup/", 1)[1]


def metric(lb: str, tg: str, name: str, stat: str, label: str) -> list:
    return [
        "AWS/ApplicationELB",
        name,
        "LoadBalancer",
        lb,
        "TargetGroup",
        tg,
        {"stat": stat, "period": 60, "label": label},
    ]


def save_graph(
    *,
    region: str,
    out_dir: pathlib.Path,
    widget_dir: pathlib.Path,
    name: str,
    title: str,
    start: str,
    end: str,
    metrics: list[list],
    y_label: str,
) -> pathlib.Path:
    widget = {
        "width": 1200,
        "height": 420,
        "start": start,
        "end": end,
        "timezone": "-0700",
        "view": "timeSeries",
        "stacked": False,
        "region": region,
        "title": title,
        "metrics": metrics,
        "yAxis": {"left": {"label": y_label, "showUnits": False}},
    }
    widget_path = widget_dir / f"{name}.json"
    png_path = out_dir / f"{name}.png"
    widget_path.write_text(json.dumps(widget, separators=(",", ":")))
    raw = subprocess.check_output(
        [
            "aws",
            "cloudwatch",
            "get-metric-widget-image",
            "--region",
            region,
            "--metric-widget",
            f"file://{widget_path}",
            "--output",
            "text",
            "--query",
            "MetricWidgetImage",
        ],
        text=True,
    )
    png_path.write_bytes(base64.b64decode(raw.strip()))
    return png_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Render CloudWatch ALB graphs for a read-path run.")
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--result-dir", required=True)
    parser.add_argument("--start", required=True, help="ISO-8601 UTC start time")
    parser.add_argument("--end", required=True, help="ISO-8601 UTC end time")
    parser.add_argument("--title-prefix", default="Read ALB")
    args = parser.parse_args()

    state = parse_state(pathlib.Path(args.state_file))
    region = state["AWS_REGION"]
    lb = alb_dimension(state["READ_ALB_ARN"])
    tg = target_group_dimension(state["READ_ALB_TARGET_GROUP_ARN"])

    result_dir = pathlib.Path(args.result_dir)
    out_dir = result_dir / "cloudwatch-graphs"
    widget_dir = out_dir / "widgets"
    widget_dir.mkdir(parents=True, exist_ok=True)

    graphs = [
        save_graph(
            region=region,
            out_dir=out_dir,
            widget_dir=widget_dir,
            name="01-request-count",
            title=f"{args.title_prefix} RequestCount, Sum per 1 minute",
            start=args.start,
            end=args.end,
            metrics=[metric(lb, tg, "RequestCount", "Sum", "requests/min")],
            y_label="requests/min",
        ),
        save_graph(
            region=region,
            out_dir=out_dir,
            widget_dir=widget_dir,
            name="02-target-http-codes",
            title=f"{args.title_prefix} Target HTTP Codes, Sum per 1 minute",
            start=args.start,
            end=args.end,
            metrics=[
                metric(lb, tg, "HTTPCode_Target_2XX_Count", "Sum", "2XX/min"),
                metric(lb, tg, "HTTPCode_Target_4XX_Count", "Sum", "4XX/min"),
                metric(lb, tg, "HTTPCode_Target_5XX_Count", "Sum", "5XX/min"),
            ],
            y_label="responses/min",
        ),
        save_graph(
            region=region,
            out_dir=out_dir,
            widget_dir=widget_dir,
            name="03-target-response-time",
            title=f"{args.title_prefix} TargetResponseTime, p95/p99/Average",
            start=args.start,
            end=args.end,
            metrics=[
                metric(lb, tg, "TargetResponseTime", "Average", "avg seconds"),
                metric(lb, tg, "TargetResponseTime", "p95", "p95 seconds"),
                metric(lb, tg, "TargetResponseTime", "p99", "p99 seconds"),
            ],
            y_label="seconds",
        ),
        save_graph(
            region=region,
            out_dir=out_dir,
            widget_dir=widget_dir,
            name="04-healthy-hosts",
            title=f"{args.title_prefix} Healthy/Unhealthy Host Count",
            start=args.start,
            end=args.end,
            metrics=[
                metric(lb, tg, "HealthyHostCount", "Minimum", "healthy min"),
                metric(lb, tg, "UnHealthyHostCount", "Maximum", "unhealthy max"),
            ],
            y_label="hosts",
        ),
        save_graph(
            region=region,
            out_dir=out_dir,
            widget_dir=widget_dir,
            name="05-connection-errors",
            title=f"{args.title_prefix} TargetConnectionErrorCount and ELB 5XX",
            start=args.start,
            end=args.end,
            metrics=[
                metric(lb, tg, "TargetConnectionErrorCount", "Sum", "target connection errors"),
                metric(lb, tg, "HTTPCode_ELB_5XX_Count", "Sum", "ELB 5XX"),
            ],
            y_label="count/min",
        ),
    ]
    manifest = {
        "result_dir": str(result_dir),
        "graph_dir": str(out_dir),
        "window_start": args.start,
        "window_end": args.end,
        "load_balancer_dimension": lb,
        "target_group_dimension": tg,
        "graphs": [str(path) for path in graphs],
    }
    (out_dir / "graphs-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    for path in graphs:
        print(path)


if __name__ == "__main__":
    main()
