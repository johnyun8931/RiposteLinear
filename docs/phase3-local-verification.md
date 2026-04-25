# Phase 3 Local Verification

Local verification run completed on 2026-04-20 before any AWS deployment.

## Reproducing It

Use the temporary local helper scripts under [script/](/Users/kevwjin/Documents/01-projects/riposte/script):

```bash
bash script/phase3-local-up.sh
bash script/phase3-local-verify.sh
bash script/phase3-local-benchmark.sh
bash script/phase3-local-down.sh
```

These scripts are the current reproducibility path for the Phase 3 local setup. They are developer helpers for the current sharding checkpoint, not the intended long-term operator interface.

`phase3-local-benchmark.sh` now performs a local server-thread sweep and writes summary artifacts under the script state directory in `/tmp/riposte-phase3-local/`.

## Topology

- single-shard baseline:
  - leader `127.0.0.1:8640`
  - follower `127.0.0.1:8641`
- sharded setup:
  - coordinator `127.0.0.1:8630`
  - shard 0 leader/follower: `127.0.0.1:8610`, `127.0.0.1:8611`
  - shard 1 leader/follower: `127.0.0.1:8620`, `127.0.0.1:8621`

## What Was Verified

- writes are rejected before epoch start:
  - coordinator upload returned `No active epoch`
- coordinated epoch start opens writes across shards:
  - coordinator and both shard leaders reported `epoch=2 state=active accepting=true`
  - all three reported the same `epoch_id`, `start`, `end`, and `duration`
- boundary routing and payload preservation:
  - uploaded `shard0-boundary` to `(row=0, col=1)`
  - uploaded `shard1-boundary` to `(row=128, col=2)`
  - `DumpPlaintext` on shard 0 leader matched the shard 0 payload at `(0,1)`
  - `DumpPlaintext` on shard 1 leader matched the shard 1 payload at `(128,2)`
- publication:
  - shard 0 wrote `/tmp/riposte-results-s0/epoch-000002-shard-0-server-0.json`
  - shard 1 wrote `/tmp/riposte-results-s1/epoch-000002-shard-1-server-0.json`
  - filenames and metadata were unambiguous across shards
  - published slot rows stayed within the owning shard range
  - published `message_hex` values matched the deterministic payload bytes
- second epoch start:
  - a second coordinated epoch started cleanly after the first completed

## Throughput Check

The local hammer comparison is now based on a full local server-thread sweep on `2026-04-20`.

When reading hammer-mode results, treat `No active epoch` as benign only if the run clearly made progress first. The current client behavior can also tolerate `No active epoch` when the epoch was never active at all, so the benchmark scripts should continue to record an estimated successful-request count from server-side accepted totals rather than relying only on client exit behavior.

- host context:
  - `host_model=Mac16,13`
  - `physical_cpu=10`
  - `logical_cpu=10`
- measured sweep:

| server_threads | baseline_total | baseline_req_per_sec | shard0_total | shard1_total | sharded_total | sharded_req_per_sec | delta | winner |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 931 | 116.38 | 438 | 435 | 873 | 109.12 | -58 | baseline |
| 2 | 1220 | 152.50 | 831 | 673 | 1504 | 188.00 | 284 | sharded |
| 4 | 1617 | 202.12 | 842 | 571 | 1413 | 176.62 | -204 | baseline |
| 8 | 1433 | 179.12 | 472 | 672 | 1144 | 143.00 | -289 | baseline |

So the local picture is mixed rather than flatly negative:
- the routed 2-shard path beat the single-shard baseline at `server_threads=2`
- the baseline still won at `1`, `4`, and `8`
- this points more toward same-machine scheduling/resource contention than a simple “coordinator always loses” result

The benchmark artifacts for this sweep are written to:
- `/tmp/riposte-phase3-local/benchmark-summary.tsv`
- `/tmp/riposte-phase3-local/benchmark-summary.md`

## Conclusion

- local correctness for the current Phase 3 cut is good enough to continue debugging performance
- local throughput is sensitive to thread budget on the same machine
- AWS is now a reasonable next validation step, though the uneven shard totals at higher thread counts still justify one lighter local follow-up if cleaner attribution is needed first
