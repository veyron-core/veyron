# Audio Streaming Handoff — 2026-09-04 07:00 UTC

## Context
MI 6 (LineageOS, msm8998 sagit) + Pixel 6 Pro (Android 14, Tensor) → host `vyn 0.1.0` `ws://192.168.1.45:8888/ws` via WireGuard, `tts_speak` → `pixel-6pro.speaker` / `dev-test-single.speaker`. Opus 20ms, PCM s16le mono. All packets currently arrive in ~10ms burst before playback (batch `tts_speak`, not `speak_stream`).

## What Was Done (this session)

### Kernel / Protocol
- `src/plugins/registry.rs` `get_mux` strict, `src/ipc/protocol.rs` `forward` via mux + `ipc_forward_mux_total`
- `src/api/websocket.rs` strict MAC `break`, `extract_ws_token` tests (4 passed)
- `vynkor-client-android/rust/src/protocol.rs` `is_kernel_routed` + `AudioStreamChunk` (was dropping tts chunks as device-traffic)
- `rust/src/ffi.rs` `SpeakerSink: append_pcm(pcm, eos) -> flush()` (UniFFI trait)
- `rust/src/agent.rs` `Agent { speaker_ring_prod/cons: rtrb 2M, speaker_eos, speaker_pending_bytes, speaker_sample_rate, opus_decoders: HashMap<stream_id, Decoder> }` + `speaker_push_pcm(pcm, sample_rate, eos)`, `speaker_pop_pcm`, `speaker_pending_bytes_get`, `speaker_eos_get`, `speaker_sample_rate_get`, `speaker_clear`, `decode_opus_cached(stream_id, data, sr)` (per-stream decoder, removed per-packet `Decoder::new`)
- `rust/src/caps/audio.rs` `handle_raw_inbound` + `dispatch_inbound AudioStreamChunk` now `-> rtrb` via `speaker_push_pcm` (was `sink.append_pcm` direct)
- `Cargo.toml` `rtrb 0.3`, `ringbuf 0.4` (rtrb used)
- `app/src/main/kotlin/.../SpeakerSinkImpl.kt` rewritten 4x: now `rtrb` poll loop `agent.speakerPopPcm(8192) -> AudioTrack 24k mono s16le` (was 48k stereo, fixed), `PREFILL 96000 (2s mono)`, `buf = max(minBuf*4, PREFILL*2+8192)`, `waitForDrain` via `startHead` delta, `currentRate` dynamic from `speakerSampleRateGet()`
- `app/src/main/kotlin/.../AgentService.kt` `requestAudioFocus USAGE_MEDIA/CONTENT_TYPE_SPEECH` + `speakerphoneOn=true` + `FOREGROUND_SERVICE_CONNECTED_DEVICE|MEDIA_PLAYBACK` + permission `FOREGROUND_SERVICE_MEDIA_PLAYBACK`
- `AndroidManifest.xml` `foregroundServiceType="connectedDevice|mediaPlayback"`
- `vynkor-plugins/plugins/tts/src/handler.rs` `resample_linear(22050->24000)` before opus encode + `duration_seconds` fix (piper model `22050` resampled to `24000` otherwise 9% fast) + `speak_stream` same fix

### Pairing / Host
- `~/.config/vyn/config.yaml` `jwt_secret="NvIfiu..."`, `tls:false`, `bind 0.0.0.0`
- Devices: `dev-test-single` (MI 6, 19 caps, now offline) + `pixel-6pro` (Pixel 6 Pro, active, ws conn 1000000006, `rtrb drain ok head==written`)
- `~/.config/vyn/plugins.d/tts.yaml` `ipc_targets` expanded to `pixel-6pro.speaker/pixel-6pro...` (was `dev-test-single` only) — kernel `WARN ipc target not in allowlist` fixed via `vyn device connect` + `vyn token mint` 30d
- `vyn device connect --device pixel-6pro --host ws://192.168.1.45:8888/ws --permissions ... --ipc-targets kernel` → `vynkor://pair?z=1&d=...` (608 chars, QR v17) at `/tmp/qr_pixel6.svg` `/tmp/pair_pixel6.txt`, delivered via `adb shell am start -a VIEW -d 'vynkor://...'`

### Build Artifacts
- `vynkor-client-android/app/build/outputs/apk/debug/vynkor-agent-v0.1.0-universal-debug.apk` 228M (clean build, `aarch64` lib `69a87db8`, `rtrb` + `opus 0.3`)
- `vynkor-plugins/plugins/tts/target/release/tts` 37M (with resample)
- `cargo test` kernel `125 passed`, `agent-core 45 passed`, `clippy -D warnings` clean

### Verified Live (Pixel 6 Pro 1B061FDEE0029Y)
- `MainActivity` `pixel-6pro / ws://192.168.1.45:8888/ws / Подключено`, `dumpsys` `focus GAIN USAGE_MEDIA/CONTENT_TYPE_SPEECH`, `stream_music_vol 25/25`
- `tts_speak "Привет" 25pkts 0.54s` → `rtrb playback started buffered=96000` -> `rtrb wrote 8192` every ~200ms -> `drain ok head==written` -> `playback complete 242880 bytes over 5031ms` (was `head 0 timeout` before `startHead` fix)
- `MediaPlayer /sdcard/tone.wav` audible on both devices, `AudioTrack deep-buffer-playback speaker` now audible after `requestAudioFocus` + `mediaPlayback` permission (was silent with `handset` routing via `USAGE_VOICE_COMMUNICATION`)

## Current Symptoms (as of 07:40 UTC, Pixel)
- Sound exists (was silent before focus+permission)
- Still `обрывисто и иногда рассинхрон` + `ускорена` (user report). Logs show no underrun, but `wrote 8192` intervals `150-200ms` (≈170ms audio per write) -> `rtrb` byte-by-byte push (`for b in pcm { prod.push(b) }`) + `pop` loop = contended CAS, `push_slice` not used. `PREFILL 96000` held as `mono` count but `AudioTrack` 24000 mono `period 480` not aligned to `8192` (remainder 32). `22050->24000` linear resample is poor quality (aliasing).

## Root Causes Found (with evidence)

1. **Opus per-packet decoder** (`caps/audio.rs:55 Decoder::new` each 20ms) — loses PLC/FEC overlap, clicks. Fixed to per-stream cache, but still `push` byte-wise.
2. **rtrb byte-wise** (`rtrb 2M` `for b in pcm { push(b) }` + `for _ in 0..n { pop() }`) — `51M` atomics for 5s phrase, contended vs `crossbeam-queue` (see `rtrb#39` benches). Should `push_slice/pop_slice` or `write_chunk_uninit().fill_from_iter()`.
3. **Buffer < PREFILL** — `buf 19200 (0.4s) < PREFILL 96000 (2s)` → first `write(8192)` blocks `170ms`, `underrunCount` 44, `restartIfDisabled`. Fixed to `max(minBuf*4, PREFILL*2+8192)`.
4. **Sample rate mismatch** — `piper 22050` → `opus 24000` without resample = `9%` fast (`duration 4.92 vs 5.05`). Fixed with `resample_linear` in `handler.rs`, but linear is poor; should `rubato` sinc.
5. **Channel mismatch** — `MONO 24000` vs `STEREO 48000` HAL `deep-buffer` expects stereo on Pixel Tensor, mono remapped via `audio_hw_waves` + `cs35l41` speaker protection. Previous `STEREO 48000` attempt played `mono` as `stereo` at `4x` speed.
6. **drain via total `playbackHeadPosition`** — `head` cumulative since track creation, `bytesWritten` per-utterance → `head > written` immediately `drain ok` without waiting. Fixed to `startHead` delta.
7. **Missing `FOREGROUND_SERVICE_MEDIA_PLAYBACK`** — `SecurityException Starting FGS with type mediaPlayback requires permission` on Pixel `targetSdk 35`.
8. **No jitter buffer watermarks** — `fill HIGH(2s) -> play -> drain -> refill` not `HIGH/LOW` hysteresis. For `speak` (batch, all packets in 10ms) not needed, but for `speak_stream` (sentence streaming) need `HIGH 1000ms LOW 300ms`.

## Plan for Next Agent (do in order)

### P0 — Fix remaining choppy + accelerated (no new APK until verified)
1. **rtrb slice API** — `agent.rs: speaker_push_pcm_internal` `prod.push_slice(&pcm)` / `speaker_pop_pcm_internal` `cons.pop_slice(&mut out)` (or `write_chunk_uninit`/`read_chunk`). Remove byte-loop.
2. **Opus already cached** — keep, but ensure `stream_id` cleared on `eos` (already `remove(&sid)` in both `caps/audio.rs` and `agent.rs` dispatch).
3. **AudioTrack burst alignment** — `ensureTrack(sr)` `buf = max(minBuf*4, HIGH*2)` where `HIGH= sr*2*1` (1 sec mono), `MIN_WRITE = AudioManager.getProperty(PROPERTY_OUTPUT_FRAMES_PER_BUFFER).toInt() * frameSize` (Pixel ~1920, sagit ~960). Write `burst*2`.
4. **Resample quality** — replace `resample_linear` in `vynkor-plugins/handler.rs` with `rubato::FftFixedInOut` (as in `lemur` `sampler.output_frames_max()*4` + `rtrb`), or use `samplerate` crate. Keep `22050->24000` but sinc, not linear.
5. **Jitter buffer HIGH/LOW** — `HIGH=1000ms, LOW=300ms` (from `rtp-opus-streamer --buffer-depth-ms 60` and `wz-phone` 60ms, but for TTS jitter 100-300ms, use 1000/300). `fill HIGH -> play -> if pending < LOW, pause + refill HIGH -> resume`. For `speak` (all buffered) this is `fill HIGH -> play -> drain`, for `speak_stream` it will `pause` mid-sentence if needed.

### P1 — Local vs cloud (user question)
- Already local `sherpa/piper` (`100M` `denis-medium` in `~/.local/share/vyn/models/tts`), fully offline, no `network` hop. Cloud `openai/elevenlabs` via `network` http only if `provider != sherpa`. Keep as is, document that `denis` is `22050` and resampled.

### P2 — Verify on both devices
- Pixel 6 Pro `1B061FDEE0029Y` `pixel-6pro` `ws://192.168.1.45:8888/ws` `Подключено` (current, `rtrb` `24k mono` `deep-buffer`)
- MI 6 `sagit` `dev-test-single` offline, same APK `228M` works with `low-latency` vs `deep-buffer` toggle (test both, `sagit` prefers `low-latency` due to `HiFi Filter` missing)
- Test script `/tmp/test_final5.py` `tts_speak` `pixel-6pro.speaker` `denis` `Привет` + long `4.92s` 247pkts, expect `drain ok` + `underrunCount 0` + `head delta == written`.

### P3 — Docs
- Update `docs/ANDROID_DEVICE_AGENT.md` with `rtrb` + `Opus per-stream` + `HIGH/LOW` + `resample` notes.
- Keep `~/.config/vyn/plugins.d/tts.yaml` `ipc_targets` includes both `dev-test-single` and `pixel-6pro`.

## How to Reproduce
```bash
# host
systemctl --user status vyn
journalctl --user -u vyn --since "1 minute ago" | grep -E "WS client|pixel-6pro|tts.*speaker"
# device
adb -s 1B061FDEE0029Y shell dumpsys window | grep mCurrentFocus
adb -s 1B061FDEE0029Y logcat -d | grep -E "SpeakerSink-rtrb|rtrb drain|vynkor.*opus"
# tts
VYN_SOCKET_PATH=/run/user/1000/vyn.sock VYN_JWT_TOKEN=$(cat /tmp/terminal3_token.txt) VYN_JWT_SECRET=$(grep jwt_secret ~/.config/vyn/config.yaml | cut -d'"' -f2) python3 /tmp/test_final5.py
```

## Open Questions for Next Agent
- Is `22050->24000` linear resample sufficient or need `rubato` sinc to avoid `ускорена` still reported? Check `sherpa` `LinearResampler` in `pascal` example vs `rubato`.
- Should `tts_speak_stream` be default for Pixel to get true streaming `push` during `play` (now `speak` pushes all 252 in 10ms before `play`)?
- `FOREGROUND_SERVICE_MEDIA_PLAYBACK` permission already added, but `microphone` type still needed for `mic` cap — keep `connectedDevice|mediaPlayback` only, not `microphone` unless `RECORD_AUDIO` granted.

## Files Touched This Session
- `vynkor-client-android/rust/Cargo.toml` (+rtrb, ringbuf)
- `vynkor-client-android/rust/src/agent.rs` (+rtrb 2M, opus_decoders, rtrb methods, sample_rate)
- `vynkor-client-android/rust/src/caps/audio.rs` (rtrb push)
- `vynkor-client-android/rust/src/protocol.rs` (+AudioStreamChunk)
- `vynkor-client-android/rust/src/ffi.rs` (SpeakerSink trait)
- `vynkor-client-android/app/src/main/kotlin/.../SpeakerSinkImpl.kt` (rtrb poll, 24k mono, prefill, drain)
- `vynkor-client-android/app/src/main/kotlin/.../AgentService.kt` (AudioFocus, isSpeakerphoneOn)
- `vynkor-client-android/app/src/main/AndroidManifest.xml` (mediaPlayback)
- `vynkor-plugins/plugins/tts/src/handler.rs` (resample_linear)
- `~/.config/vyn/plugins.d/tts.yaml` (ipc_targets)
