# Wheelzone Core — System Docs (Bot1 + Bot2 + SQLite)

This repository contains **documentation artifacts** for a production messaging pipeline implemented as **Bot1 + Bot2 + SQLite**.
The documents are **facts-only** and describe the current operational state.

## Documents

- `docs/System_Overview.md` — authoritative technical overview (facts only)
- `docs/Delivery_State_Machine.md` — formal RFC-style delivery state machine (authoritative)
- `docs/diagrams/sequence.mmd` — Mermaid sequence diagram for end-to-end processing

## System Shape
```text
[Inbound Platform/API]
        |
        v
     (Bot1)
  normalize + persist
        |
        v
     SQLite DB
  events + delivery
        |
        v
     (Bot2)
 claim/lease + dispatch
        |
        v
[Delivery Channels]
```

## Mermaid

To render the diagram:
- open `docs/diagrams/sequence.mmd` in any Mermaid-compatible viewer/editor.

