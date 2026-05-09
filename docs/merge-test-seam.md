# Merge Test Seam

`db.Server` exposes `mergeFn` as an internal seam around the real merge path.

Production behavior:

- `mergeFn` defaults to `sendMergeRequest`
- epoch completion calls `mergeFn`
- `sendMergeRequest` performs the real RPC-based merge, stores plaintext, and publishes the result file

Why this wrapper exists:

- epoch lifecycle tests need to drive `active -> merging -> completed`
- those tests should not need live peer RPC connections or a full merge run
- replacing `mergeFn` in tests lets the control-plane transition be verified independently from the networked merge path

This is a testing seam, not a separate production code path.
