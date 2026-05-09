# Riposte Linear: Read Path And Failover Branch

This branch is focused on the **read path** for the linear Riposte implementation. It builds on the modern `linear` branch of Riposte and includes the newly added read-serving and failover features from the failover work.

The original `linear` branch contains the modern version of the Riposte code used in the variant of Riposte that appears in Henry Corrigan-Gibbs' [PhD dissertation](https://purl.stanford.edu/nm483fv2043). This repository keeps that write path as the base system, then adds a stateless read-serving layer for evaluating read scalability and read-server recovery.



An explanation of the branches in this repository is [here](https://bitbucket.org/henrycg/riposte/).

## Branch focus

The main additions in this branch are:

- Shard leaders can publish the merged epoch table to S3 after each merge.
- Published read tables use stable S3 keys so the latest epoch overwrites the previous epoch.
- A new `readserver` binary loads shard tables from S3 into memory and serves HTTP reads.
- Read requests use `(x,y)` coordinates, where `y` maps to the owning shard and `x` selects the message slot within that row.
- Read servers expose `/healthz`, `/status`, and `/read?x=<column>&y=<global_row>` endpoints.
- The AWS evaluation path adds a public Application Load Balancer for read traffic.
- Read servers run in an Auto Scaling Group so a crashed read server is replaced automatically.
- Failover scripts collect ALB target health, readserver status, CloudWatch graphs, and sustained read-load evidence.

The write path remains the existing coordinator/server path. The read path is intentionally separated: writes produce epoch tables, shard leaders publish those tables to S3, and stateless read servers consume the latest published table.

## Read-path AWS evaluation

The read-path evaluation scripts live under `aws-eval/`.

Important scripts include:

- `12-validate-read-failover.sh`: publishes a deterministic read table, runs sustained read load through the read ALB, terminates one read server, verifies reads continue, and waits for ASG replacement.
- `13-run-read-load.sh`: runs a flat read-load benchmark through the read ALB.
- `14-run-read-load-profile.sh`: runs a longer staged read profile with baseline, spike, and cooldown phases.
- `render-cloudwatch-graphs.py`: renders CloudWatch graphs for request count, HTTP response codes, target response time, healthy host count, and connection errors.

Results and report-ready notes are saved under `aws-eval/results/`.


## How to build


1. Make sure that you have `go` installed:
```
go version
```

2. Clone the repository:
```
git clone https://bitbucket.org/henrycg/riposte/
```

3. Build the `client` and `server` binaries:
```
cd riposte
cd client 
go build
cd ..
cd server
go build
cd ..
```

4. Now you should be able to run 
```
server/server -help
client/client -help
```
to run the client and server and see the command-line options.

The read path also adds these binaries:

```
cd readserver
go build
cd ..
cd readload
go build
cd ..
```

`readserver` serves HTTP reads from S3-published tables. `readload` is the load generator used by the AWS read-path benchmarks.

## Current implementation notes

There are two important correctness caveats in the current write-validation path:

1. Proof-validation failures are logged but do not currently flip the commit decision to
   `false`. In `db/server.go`, `submitPrepares()` logs failed checks and still returns
   `true` as `shouldCommit`.

2. The bogus-write rollback path is intentionally unfinished. In `db/server.go`, `Commit()`
   panics on `!com.Commit` and includes an `XXX` comment explaining that a production
   implementation would need to expand the DPF key and XOR the malformed update back out
   of the table shares.
