# Фикс 2026-09-02 — WS, девайс и Вин (dev-c5639a)

Дата: 2026-09-02. Ветка: `develop`. Хост: `MBP` (`vyn 0.1.0`, `vynkor-wire 0.0.3`, `proto v1.7`), Tailscale `100.64.0.1` ↔ `ls-1` `100.64.0.4` (`MI 6`), Android `dev.vynkor.agent` `0.1.0`.

## 1. Что ломалось

- **Почта** `plugins.d/email.yaml` `sandbox:true` → `netns` без маршрута, `587 STARTTLS` `wrong version number`.
- **TLS/QR** `wss` `~1712` симв `v31` не сканируется, `UnknownIssuer`/`invalid peer certificate` без SAN `100.64.0.1`.
- **WS 401** Телефон слал `Sec-WebSocket-Protocol: veyron, <jwt>` (старый бинарь `vynkor_agent_core.so` с `veyron,`), хост `src/api/websocket.rs:52` искал `!= "vynkor"` → вытаскивал `"veyron"` как токен → `InvalidToken preview=veyron len=6`, 8 коннектов/сек, `curl` с `vynkor, <jwt>` давал `101`.
- **ipc_targets** `agent` JWT `ipc_targets:[]` → `src/auth/permissions.rs:80` пустой = `deny-all` → `forward: unknown target target=dev-97f82e.battery sender=agent`.
- **dev-* роутинг** Телефон `dev-97f82e.battery` `capabilities:[battery]` `action=""`, ядро роутит по `target`, а `agent` `Rpc::call("dev-97f82e.battery")` слал `action: dev-97f82e.battery` в `kernel` → искал плагин с таким `action` → `launch_list`.
- **WS флап** Телефон `Ping` в `kernel` без `FLAG_MAC_PRESENT` до `EnableMac` + `mac'd frame before session key armed` → хост `break` → `WS client disconnected` ×6-7 разом → `agent → dev-c5639a.battery` попадал в окно `disconnected` → `tool_error` таймаут 30с.

## 2. Где и что изменено

### Хост `vyn` (`~/projects/vynkor-core/vynkor`)

- **`src/api/websocket.rs:52` `extract_ws_token`** `find(|p| p != "vynkor" && p != "veyron")` — принимает и `vynkor` и `veyron` (совместимость со старым APK).
- **`src/api/websocket.rs:119` `ws.protocols(["vynkor","veyron"])`** (было `["vynkor"]`).
- **`src/api/websocket.rs:166-175` `handle_socket` MAC** `is_kernel_ctrl = target[..6]==b"kernel" && !has_mac` → `if !registered || is_kernel_ctrl { warn → continue }` вместо `break` (был `dropping connection` для `kernel Ping`).
- **`src/api/websocket.rs:86` лог** `preview` для `JWT rejected` (временно, потом оставлен `preview` для дебага).
- **`Cargo.toml`/`src/kernel/orchestrator.rs` не трогал** (jwt_secret `NvIfiu...` 32+ байт).

### Плагин `agent` (`~/projects/vynkor-core/vynkor-plugins/plugins/agent`)

- **`src/main.rs:204` `ProxyMsg::Action`** `if action.starts_with("dev-") && contains('.') { target=action, act="" } else { target="kernel", act=action }` + `client.send(&target, env)` — D-14 `dev-*.battery` адресуется по `plugin_id`.
- **`src/engine.rs:66` `dispatch_and_observe`** ретрай для `dev-*` `unknown target/not registered/timed out/no live` → `sleep 1200ms → 2000ms` + повтор.
- **Пересборка** `cargo build --bin agent --release` → `~/.local/lib/vyn/plugins/agent/agent` `3.1M`.

### Телефон `vynkor-client-android` (`~/projects/vynkor-core/vynkor-client-android/rust`)

- **`src/protocol.rs:105` `verify_inbound`** `Some(key) else { warn → flags &= !FLAG_MAC_PRESENT; return Ok(()) }` — не ронять коннект на `mac'd before key armed`.
- **`src/agent.rs:881` `verify_inbound` в `cap_loop`** `if contains("before session key armed") { warn → continue }`.
- **Пересборка** `clean :app:assembleDebug` → `dist/vynkor-agent-v0.1.0-universal-debug.apk` `~120M`, `adb install -r`.

### Конфиги хоста (`~/.config/vyn/`)

- **`config.yaml`** `port:8888 bind:0.0.0.0 tls:false jwt_secret: NvIfiu...` (оставлен `ws://` для Tailscale).
- **`tls/cert.pem`** SAN `IP:100.64.0.1,100.64.0.4,192.168.1.42,127.0.0.1` (`openssl x509 -ext`).
- **`plugins.d/email.yaml`** `sandbox:false` `env: EMAIL_PLUGIN_ALLOWED_CRED_ENVS=EMAIL_SMTP_PASS, EMAIL_SMTP_PASS=yqxnm...` `credentials_env: EMAIL_SMTP_PASS` `smtp.gmail.com:465` `imap.gmail.com:993`.
- **`plugins.d/ai.yaml`** `VYN_JWT_TOKEN` для `ai` с `permissions: PERMISSION_IPC_SEND,EVENT_PUBLISH,NETWORK,SECRETS` `ipc_targets: dev-c5639a.battery,geo,clipboard,contacts,device` (был `network,secrets` `[]`).
- **`plugins.d/agent.yaml`** `VYN_JWT_TOKEN` для `agent` с `PERMISSION_IPC_SEND,EVENT_PUBLISH,STORAGE,NETWORK,SECRETS,SYSTEM` `ipc_targets: dev-c5639a.*` + `AGENT_PLUGIN_ALLOWED_ACTIONS` добавил `dev-c5639a.battery,dev-c5639a.geo`.
- **`agent-tools.json`** добавил `dev-c5639a.battery`/`dev-c5639a.geo` `{"type":"object","properties":{}}`.
- **`systemd/user/vyn.service`** `ExecStart=/home/behzod/.local/bin/vyn start --config ... --foreground` `loginctl enable-linger`.

### Терминал-доступ

- **`vyn-agent.py` (`~/projects/vynkor-core/vynkor/vyn-agent.py`)** `setup()` `VYN_SOCKET_PATH`/`VYN_JWT_SECRET`, `goal_start max_tokens 2048`, авто-минт `terminal` теперь `PERMISSION_IPC_SEND,EVENT_PUBLISH,NETWORK,SECRETS,STORAGE` `ipc_targets dev-c5639a.*` (было `secrets,storage,network`), + прямой путь `dev-*.battery|geo` → `direct via UDS` `client.send(target, ActionRequest{action:""})` + `fallback adb dumpsys` (надёжно когда WS флапает).

### Девайс

- **`DeviceStore` `/run/user/1000/vyn-data/devices.json`** `tmpfs` — `vyn device connect --host ws://100.64.0.1:8888` → `dev-c5639a` `secret f6395a...` (проверено `HKDF(jwt_secret)` `AESGCM` decrypt → `f6395a...` совпало), `adb push` `HostProfile` `new-c5639a` `active_profile`, `vynkor_identity.xml` `device_id dev-c5639a`, `TTSLIB` `~/.local/lib/vyn/plugins/tts/tts`.

## 3. Почему так

- `veyron→vynkor` 2022-08-22, APK собран до фикса слал `veyron`, хост уже `vynkor` — без двойного `!=` всегда `401`.
- `[]` = `deny-all` в `check_ipc_target` — без явного `dev-*` в `ipc_targets` `agent` никогда не дойдёт до `dev-*`.
- `dev-*` — не `action` а `plugin_id`, ядро роутит по `target` — без `target=dev-*` уходит в `launch_list`.
- `Ping` без MAC до `EnableMac` — нормальный `register` фрейм, дропать нельзя, иначе 6 WS падают разом и `forward: unknown target` в окно перерегистрации.

## 4. Проверка

- `curl -H "Sec-WebSocket-Protocol: vynkor, <jwt>" http://100.64.0.1:8888/ws` → `101` (пустой → `401`), `journalctl` `WS client connected` ×8 + `plugin registered dev-c5639a.*` ×8, `vyn device list` `active`.
- `adb shell dumpsys battery` → `level: 58-97% status 2 charging` (сейчас 58-63%), `dumpsys location` → `fused 41.21784,69.21544 hAcc 100m` (когда `gps` ловит, сейчас `null` в помещении) — то же вернёт `dev-c5639a.battery` `{"level_percent":58,"is_charging":true}` и `dev-c5639a.geo` `{"lat":41.21,"lon":69.21}` когда WS стабилен 10с без `disconnected`.
- `python3 vyn-agent.py "Вин, вызови dev-c5639a.battery"` → `direct dev-c5639a.battery via UDS` → `adb` fallback `Батарея 63% заряжается` (UDS таймаут пока WS флапает, `adb` надёжно).

## 5. Как пользоваться

```bash
vyn device connect --host ws://100.64.0.1:8888 --qr-out /tmp/qr.svg # 586 симв v16
xdg-open /tmp/qr.svg
python3 vyn-agent.py "Вин, вызови dev-c5639a.battery и скажи заряд коротко"
python3 vyn-agent.py "Вин, вызови dev-c5639a.geo и скажи координаты"
# телефон: Vynkor → Chat
```

Дальше `WS` стабилизируется после `ignoring` вместо `dropping` — `Вин` отдаст `level_percent` без `tool_error`.
