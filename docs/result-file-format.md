# Result File Format Notes

Published result files currently include:

- epoch metadata:
  - `epoch_id`
  - `start_time`
  - `end_time`
  - `duration_seconds`
  - `completed_at`
- instance metadata:
  - `shard_id`
  - `server_index`
- table shape and verification context:
  - `table_height`
  - `table_width`
  - `slot_length`
  - `non_zero_slot_count`
- sparse merge output:
  - `slots`

## Why `message_hex` uses hex

Published result files store slot payloads as hex strings rather than raw bytes or text.

Reasoning:

- result files are JSON, so the slot payload needs a text-safe representation
- slot contents are arbitrary bytes, not guaranteed to be UTF-8 text
- hex is simple, deterministic, and easy to inspect in debugging or evaluation workflows
- this file format is currently a verification artifact, so readability and stability matter more than compactness

`message_hex` therefore means:

- the underlying slot payload is binary
- it has been serialized to lowercase hexadecimal text for publication

Base64 would also work, but hex was chosen because it is more direct to inspect in crypto- and systems-oriented debugging.
