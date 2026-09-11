# Vynkor Frame Flag Bit Space

This document is the **single source of truth** for all flag bits in the Vynkor wire protocol.
No other file may define flag constants; all SDKs import the values defined here.

## Flag Bit Table

| Bit | Hex    | Constant         | Meaning |
|-----|--------|------------------|---------|
| 0   | 0x0001 | FLAG_MAC_PRESENT | 32-byte HMAC-SHA256 tag appended after payload |
| 1   | 0x0002 | FLAG_COMPRESSED  | Payload zstd-compressed on the wire (implemented — see below) |
| 2   | 0x0004 | FLAG_FRAGMENTED  | Frame is one fragment of a larger message (implemented — see below) |
| 3   | 0x0008 | FLAG_PRIORITY    | High-priority system frame (reserved, not yet implemented) |
| 4   | 0x0010 | FLAG_RAW_BINARY  | Payload is raw bytes (PCM or Opus); router skips Protobuf decode |
| 5–15 | —     | —                | Reserved |

### FLAG_COMPRESSED (Bit 1)

Implemented on the UDS path. The kernel's write path (`write_frame_raw`) transparently
zstd-compresses any non-raw-binary payload ≥ 64 KiB (`COMPRESS_THRESHOLD`) when the
compressed form is smaller, sets this flag, and rewrites `length`/`crc32` to describe
the wire (compressed) bytes. The read path decompresses and **normalizes**: after
`read_frame` returns, `payload` is always plaintext and `flags`/`length`/`crc32`
describe the plaintext.

**MAC interaction:** on secured connections the HMAC tag is computed over the
*plaintext* header and payload (before compression). Receivers must therefore verify
the tag against the normalized (decompressed) header/payload, not the raw wire bytes.

> **SDK status:** all three SDKs (Rust, Python, C++) decompress and normalize
> correctly (R5-01 ✓): payload is always plaintext after the read call, and MAC
> verification (when a session key is supplied) runs against the rebuilt plaintext
> header. Python depends on `zstandard`; C++ links `libzstd` via pkg-config. The
> WebSocket gateway normalizes `FLAG_COMPRESSED` through the shared wire reader
> (MA-13) — see below.

### MAC Failure Wire Behavior (R5-12)

On a secured connection (kernel started with `jwt_secret`), every frame after
registration must carry a valid `FLAG_MAC_PRESENT` tag. When one doesn't, the
kernel sends an `ErrorMessage` — `ERR_MAC_MISSING` if the frame carries no tag
at all, `ERR_MAC_INVALID` if a tag is present but fails verification — then
drops the connection. The error is sent CRC-only (no MAC), best-effort: the
kernel does not wait for delivery before closing. This replaces silently
dropping the connection with no explanation, which made client-side debugging
of a missing/rotated secret indistinguishable from a network failure.

### FLAG_FRAGMENTED (Bit 2)

Implemented on the UDS path (kernel side). The first 10 bytes of the payload are the
fragment header: `[fragment_id: u16][sequence: u16][total: u16][stream_id: u32]`,
all big-endian. The kernel reassembles per `stream_id` with these bounds: max 64
concurrent streams per connection, reassembled size ≤ 1 MiB (`MAX_PAYLOAD_SIZE`),
incomplete sets discarded after 30 s. Violations drop the connection. Rejected on
the WebSocket gateway (R5-03 ✓) — see below.

> **SDK status:** Rust SDK implements both sides — `VynkorClient::send_fragmented`
> emits spec-conformant fragments (each individually MAC'd when secured), and
> `recv`/`recv_frame` reassemble inbound fragments with the same bounds as the kernel.
> C++ SDK now implements fragmentation too: `absorb_fragment`/bounded reassembly plus
> `send_fragmented`, gated by `MAX_REASSEMBLY_STREAMS`/`MAX_PAYLOAD_SIZE` (fixed per
> AUDIT M11, 2026-08-11). Python SDK still does not implement fragmentation.

### FLAG_RAW_BINARY (Bit 4)

When set, the payload is raw binary audio data (PCM_S16LE or Opus). The kernel routes
the frame without Protobuf decode. Stream metadata must be negotiated out-of-band via an
`AudioStreamChunk` message on a prior frame.

Plugins that send frames with `FLAG_RAW_BINARY` set must hold `PERMISSION_AUDIO_STREAM`.
See [Audio Permissions](#audio-permissions) below.

## WebSocket Gateway Inbound Frame Support (R5-03)

The WebSocket gateway parses inbound binary messages with the kernel's single
frame parser (`crate::ipc::framing::parse_frame`, MA-13), which delegates to
`vynkor_wire::framing::read_frame` — the same reader the UDS path uses.

- `FLAG_COMPRESSED` is **decompressed and normalized** exactly as on the UDS
  read path: the payload is plaintext and flags/length/crc32 describe the
  plaintext, so MAC verification and routing operate on the same normalized
  form. (Previously the gateway rejected compressed frames rather than
  normalize them; MA-13 removed the duplicate parser that forced that.)
- `FLAG_FRAGMENTED` is still **rejected** with a parse error (counted against
  the existing `MAX_WS_PARSE_ERRORS` budget): neither the gateway nor the
  `role: client` bridge has a fragment-reassembly layer (WebSocket already
  frames messages), so a fragment would reach the router half-parsed.

This does not affect kernel→WS outbound frames, which are never compressed (the
gateway does not call `write_frame_raw`).

## WebSocket JWT Delivery

**WebSocket JWT delivery:** The implementation uses
`Sec-WebSocket-Protocol: vynkor, <jwt>` for WebSocket auth. This is intentional: tokens in URL
query strings appear in server access logs, browser history, and proxy logs.
The header approach is superior. The manifesto text is superseded by this document.
Third-party clients must use the `Sec-WebSocket-Protocol` header.

## WebSocket Registration Deadline (D-07)

A JWT-authenticated WS connection that never completes `PluginRegister` never
receives a session frame-MAC key, so its frames are only CRC-protected. The
network path is TLS by default (D-07), so a wire MITM can no longer inject such
frames — and the gateway closes the residual window by dropping any
authenticated connection that has not registered within
`ws_register_timeout_secs` (default 10). Registration arms the MAC exactly as
on UDS: the ack carries a `session_nonce`, both sides derive the session key,
and every subsequent frame must carry a valid `FLAG_MAC_PRESENT` tag
(`ERR_MAC_MISSING`/`ERR_MAC_INVALID` otherwise, same as the UDS path). The
register frame itself is MAC-free by construction (the key does not exist yet)
— on the WS path it is protected by TLS; on UDS by socket permissions
(`0o600`).

## Audio Permissions

`PERMISSION_AUDIO_STREAM` — required for any plugin that sends frames with `FLAG_RAW_BINARY`
set, or sends `AudioStreamChunk` messages to another plugin.

Note: `PERMISSION_AUDIO` (value 5) is separate — it covers `play_audio` / `record_audio`
actions via `ActionRequest`. `PERMISSION_AUDIO_STREAM` gates peer-to-peer raw audio frame
routing.

## Audio Transport Convention

- **Local plugin-to-plugin over UDS:** prefer `PCM_S16LE` + `FLAG_RAW_BINARY`. Zero transcoding.
- **Plugin-to-external-client over WebSocket gateway:** prefer `OPUS`. Gateway transparently
  forwards; transcoding is the sending plugin's responsibility.
- Kernel never chooses codec. Kernel is dumb.
