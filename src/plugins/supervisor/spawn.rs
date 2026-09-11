use super::*;

use crate::plugins::metrics::drain_to_log;
use std::process::Stdio;
use std::sync::atomic::Ordering;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::{info, warn};

impl PluginSupervisor {
    pub(crate) async fn spawn_internal(
        &self,
        config: PluginConfig,
        restart_count: u32,
        replace_epoch: Option<u64>,
    ) -> Result<PluginProcess, VynkorError> {
        // B3: a manual start must never clobber a live entry. A supervised
        // restart carries a token (Some) — it replaces its own dead entry;
        // a route-level start has none and refuses while one is registered.
        if replace_epoch.is_none() && self.entries.contains_key(&config.plugin_id) {
            return Err(VynkorError::PluginAlreadyRunning(config.plugin_id.clone()));
        }

        let epoch = self.next_epoch.fetch_add(1, Ordering::Relaxed) + 1;

        #[cfg(target_os = "linux")]
        let use_shim = config.sandbox;
        #[cfg(not(target_os = "linux"))]
        let use_shim = {
            if config.sandbox {
                warn!(
                    plugin_id = %config.plugin_id,
                    "sandbox requested, but pid-namespace isolation is linux-only — running unsandboxed",
                );
            }
            false
        };
        // sandboxed plugins run under a shim that places them in a private
        // PID namespace (R9-02, see plugins::shim) — the shim is our own
        // binary re-exec'd with the hidden __shim subcommand
        // Grant the plugin a writable data dir (VYN_DATA_DIR) for its own
        // persistent state. Created up front: a sandboxed plugin cannot mkdir
        // paths it can't see, and Landlock needs the dir in RW_PATHS.
        let mut writable_paths = config.writable_paths.clone();
        let mut plugin_data_dir: Option<PathBuf> = None;
        if let Some(data_dir) = &self.data_dir {
            let plugin_data = data_dir.join("plugins").join(&config.plugin_id);
            match std::fs::create_dir_all(&plugin_data) {
                Ok(()) => {
                    plugin_data_dir = Some(plugin_data.clone());
                    writable_paths.push(plugin_data);
                }
                Err(e) => {
                    warn!(plugin_id = %config.plugin_id, error = %e, "failed to create plugin data dir");
                }
            }
        }

        let mut cmd = if use_shim {
            let mut c = Command::new(sandbox_shim_bin());
            c.arg("__shim").arg(&config.binary_path);
            // the shim mirrors the supervisor's SIGTERM→SIGKILL grace: a
            // handler-less plugin (PID 1 of its namespace) drops SIGTERM, so
            // the shim escalates on this deadline instead of blocking waitpid
            if config.grace_seconds > 0 {
                c.env("VYN_SHIM_GRACE_SECS", config.grace_seconds.to_string());
            }
            // R9-03: pass the Landlock filesystem restriction down to the
            // shim, which applies it in the plugin's pre_exec (fail-closed).
            // `full` sends no vars — the shim then builds no ruleset.
            if config.max_fs_access != crate::plugins::fsaccess::FsAccessMode::Full {
                use crate::plugins::fsaccess;
                c.env("VYN_MAX_FS_ACCESS", config.max_fs_access.as_str())
                    .env(
                        "VYN_RO_PATHS",
                        fsaccess::join_paths_env(&config.readonly_paths),
                    )
                    .env("VYN_RW_PATHS", fsaccess::join_paths_env(&writable_paths));
            }
            c
        } else {
            if config.max_fs_access != crate::plugins::fsaccess::FsAccessMode::Full {
                warn!(
                    plugin_id = %config.plugin_id,
                    "max_fs_access is only enforced for sandboxed plugins (sandbox: true) — running without filesystem restriction",
                );
            }
            Command::new(&config.binary_path)
        };
        cmd.args(&config.args)
            .env("VYN_SOCKET_PATH", &self.socket_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if let Some(dir) = &plugin_data_dir {
            cmd.env("VYN_DATA_DIR", dir);
        }
        for kv in &config.env {
            if let Some((k, v)) = kv.split_once('=') {
                cmd.env(k, v);
            }
        }
        let max_procs = config
            .max_procs
            .unwrap_or(crate::plugins::runner::DEFAULT_MAX_PROCS);
        let max_vmem_mb = config
            .max_vmem_mb
            .unwrap_or(crate::plugins::runner::DEFAULT_MAX_VMEM_MB);
        // DEBUG kill-switches (sherpa supervised-stall bisection): set on the
        // KERNEL process env, not per plugin.
        let dbg_skip_cgroup = std::env::var("VYN_DEBUG_SKIP_CGROUP").as_deref() == Ok("1");
        let dbg_skip_rlimits = std::env::var("VYN_DEBUG_SKIP_RLIMITS").as_deref() == Ok("1");
        if dbg_skip_cgroup || dbg_skip_rlimits {
            warn!(
                "DEBUG: resource-limit bisection active (skip_cgroup={dbg_skip_cgroup}, skip_rlimits={dbg_skip_rlimits})"
            );
        }
        // R9-01: per-plugin process accounting via cgroup v2 `pids.max`. The
        // scope is prepared here (parent side) so a failure degrades to the
        // RLIMIT_NPROC fallback without killing the spawn; the child only
        // moves itself into the prepared scope in `pre_exec`.
        #[cfg(target_os = "linux")]
        let cgroup_path = if dbg_skip_cgroup {
            None
        } else {
            crate::plugins::runner::prepare_pids_cgroup(&config.plugin_id, max_procs)
        };
        #[cfg(not(target_os = "linux"))]
        let cgroup_path: Option<PathBuf> = None;
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::process::CommandExt;
            let sandbox = config.sandbox;
            let cgroup_for_pre_exec = cgroup_path.clone();
            unsafe {
                cmd.as_std_mut().pre_exec(move || {
                    if dbg_skip_rlimits {
                        return Ok(());
                    }
                    if sandbox {
                        crate::plugins::runner::sandbox_pre_exec(
                            max_procs,
                            max_vmem_mb,
                            cgroup_for_pre_exec.as_deref(),
                        )
                    } else {
                        crate::plugins::runner::apply_resource_limits(
                            max_procs,
                            max_vmem_mb,
                            cgroup_for_pre_exec.as_deref(),
                        )
                    }
                });
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (max_procs, max_vmem_mb, &cgroup_path);
            if config.sandbox {
                warn!(
                    plugin_id = %config.plugin_id,
                    "sandbox=true has no effect on this OS (Linux required for namespace isolation)"
                );
            }
            warn!(
                plugin_id = %config.plugin_id,
                "resource limits (max_procs/max_vmem_mb) unsupported on this OS"
            );
        }

        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(e) => {
                // spawn can fail before the child ever forks (EAGAIN on
                // fork, EMFILE on the stdout/stderr pipes) — the prepared
                // pids scope would leak forever because the watcher task
                // that normally cleans it up is never spawned.
                #[cfg(target_os = "linux")]
                if let Some(cg) = &cgroup_path {
                    crate::plugins::runner::cleanup_pids_cgroup(cg);
                }
                return Err(VynkorError::Io(e));
            }
        };

        let plugin_id = config.plugin_id.clone();

        let log_buf = Arc::new(Mutex::new(VecDeque::<String>::with_capacity(
            self.max_log_lines,
        )));
        self.log_buffers
            .insert(plugin_id.clone(), Arc::clone(&log_buf));
        let max_lines = self.max_log_lines;

        // With the shim, stdout is the pid channel: the first line is the
        // plugin's host pid, printed only after the plugin signalled
        // readiness. EOF or a timeout means the sandbox failed to come up —
        // fail the spawn so a plugin never runs unisolated.
        let (pid, shim_pid) = if use_shim {
            let mut lines = BufReader::new(child.stdout.take().expect("piped stdout")).lines();
            let line = tokio::time::timeout(Duration::from_secs(15), lines.next_line())
                .await
                .map_err(|_| {
                    VynkorError::Internal(
                        "sandbox shim did not report a plugin pid within 15s".into(),
                    )
                })?
                .map_err(VynkorError::Io)?;
            let plugin_pid = match line.as_deref().and_then(|l| l.trim().parse::<u32>().ok()) {
                Some(p) if p > 0 => p,
                _ => {
                    // the shim died before the plugin entered the sandbox
                    // (e.g. unprivileged user namespaces disabled) — kill the
                    // shim and clean up its pids scope
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    #[cfg(target_os = "linux")]
                    if let Some(cg) = &cgroup_path {
                        crate::plugins::runner::cleanup_pids_cgroup(cg);
                    }
                    return Err(VynkorError::Internal(format!(
                        "sandbox shim exited before the plugin started (line: {line:?})"
                    )));
                }
            };
            let shim_pid = child
                .id()
                .ok_or_else(|| VynkorError::Internal("no shim pid".into()))?;
            // leftover shim stdout (should be empty) drains like a normal stream
            let buf = Arc::clone(&log_buf);
            tokio::spawn(async move {
                while let Ok(Some(line)) = lines.next_line().await {
                    let mut locked = buf.lock().await;
                    if locked.len() >= max_lines {
                        locked.pop_front();
                    }
                    locked.push_back(line);
                }
            });
            (plugin_pid, Some(shim_pid))
        } else {
            let pid = child
                .id()
                .ok_or_else(|| VynkorError::Internal("no pid".into()))?;
            let mirror = std::env::var("VYN_DEBUG_SKIP_RLIMITS").as_deref() == Ok("1")
                || std::env::var("VYN_DEBUG_SKIP_CGROUP").as_deref() == Ok("1");
            if let Some(stdout) = child.stdout.take() {
                drain_to_log(stdout, Arc::clone(&log_buf), max_lines, mirror);
            }
            (pid, None)
        };

        let mirror_stderr = std::env::var("VYN_DEBUG_SKIP_RLIMITS").as_deref() == Ok("1")
            || std::env::var("VYN_DEBUG_SKIP_CGROUP").as_deref() == Ok("1");
        if let Some(stderr) = child.stderr.take() {
            drain_to_log(stderr, Arc::clone(&log_buf), max_lines, mirror_stderr);
        }

        // Verify the child landed in its pids cgroup. The join happens in
        // `pre_exec` before exec, so `/proc/<pid>/cgroup` is authoritative by
        // the time spawn() returns; a mismatch means the RLIMIT_NPROC
        // fallback is in effect and the operator should know why.
        #[cfg(target_os = "linux")]
        if let Some(cg) = &cgroup_path {
            let expected = cg
                .strip_prefix("/sys/fs/cgroup")
                .unwrap_or(cg)
                .to_string_lossy()
                .into_owned();
            match std::fs::read_to_string(format!("/proc/{pid}/cgroup")) {
                Ok(contents) if contents.trim_end().ends_with(&expected) => {
                    info!(
                        plugin_id = %plugin_id,
                        cgroup = %expected,
                        "plugin joined per-plugin pids cgroup"
                    );
                }
                Ok(contents) => {
                    warn!(
                        plugin_id = %plugin_id,
                        cgroup = %expected,
                        actual = %contents.trim(),
                        "plugin did not join its pids cgroup — RLIMIT_NPROC fallback in effect"
                    );
                }
                Err(_) => {
                    // child already gone (e.g. an instantly-exiting test
                    // binary) — nothing to verify; the watcher cleans up
                }
            }
        }

        info!(plugin_id = %plugin_id, pid = pid, restart_count = restart_count, "plugin spawned");
        let (exited_tx, exited_rx) = watch::channel(false);
        self.entries.insert(
            plugin_id.clone(),
            PluginEntry {
                config,
                restart_count,
                pid,
                shim_pid,
                epoch,
                exited: exited_rx,
            },
        );

        let tx = self.event_tx.clone();
        let id = plugin_id.clone();
        #[cfg(target_os = "linux")]
        let cgroup_for_cleanup = cgroup_path.clone();
        tokio::spawn(async move {
            let status = child.wait().await;
            // orphan-gap sweep before cleanup: a shim killed outright
            // (watchdog SIGKILL, SIGKILL deadline) never reaps the plugin, so
            // its death signal may never have been delivered — a still-living
            // plugin would keep the scope populated and the rmdir below fail
            if shim_pid.is_some() {
                use nix::sys::signal::{kill, Signal};
                use nix::unistd::Pid;
                if kill(Pid::from_raw(pid as i32), None).is_ok() {
                    let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
                }
            }
            #[cfg(target_os = "linux")]
            if let Some(cg) = cgroup_for_cleanup {
                // a just-SIGKILLed zombie takes a beat to leave the cgroup;
                // retry briefly instead of leaking the scope dir. a fresh
                // spawn of the same plugin id reuses the scope either way
                for _ in 0..10 {
                    crate::plugins::runner::cleanup_pids_cgroup(&cg);
                    if !cg.exists() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            let success = status.map(|s| s.success()).unwrap_or(false);
            // B2: the tree is reaped and the scope is gone — publish the exit
            // so a stop awaiting `exited` resolves (a fast exit must not hang
            // the awaiter: changed() resolves on the already-published value).
            let _ = exited_tx.send(true);
            let _ = tx
                .send(ExitEvent {
                    plugin_id: id,
                    epoch,
                    pid,
                    success,
                })
                .await;
        });

        Ok(PluginProcess { plugin_id, pid })
    }
}
