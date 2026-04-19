# Result File Format Notes

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
