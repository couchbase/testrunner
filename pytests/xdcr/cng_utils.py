"""
Utility module for managing Couchbase Next Generation (CNG / stellar-gateway)
on remote VMs for XDCR CNG integration tests.

CNG (stellar-gateway) acts as a gateway proxy in front of Couchbase Server,
supporting only full TLS connections. Replications to a CNG instance require
the couchbase2:// protocol prefix.
"""

import base64
import re
import time
import logger

from remote.remote_util import RemoteMachineShellConnection

log = logger.Logger.get_logger()

STELLAR_GATEWAY_REPO = "https://github.com/couchbase/stellar-gateway.git"
STELLAR_GATEWAY_INSTALL_DIR = "/opt/cng/stellar-gateway"
STELLAR_GATEWAY_BINARY = "/opt/cng/stellar-gateway/stellar-gateway"

CNG_DEFAULT_PORT = 18098
PORT_READY_TIMEOUT = 30
PORT_READY_INTERVAL = 2

# Validation patterns for values interpolated into shell commands.
_UNIT_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_CPU_QUOTA_RE = re.compile(r"^\d+%$")
_MEMORY_MAX_RE = re.compile(r"^\d+[KMG]?$")


class CNGHelper:
    """Manages stellar-gateway installation, build, and lifecycle on remote VMs.

    Supports use as a context manager so cleanup runs on any exception path::

        with CNGHelper(server, cert, key) as cng:
            cng.install_and_build()
            cng.start()
            ...
    """

    MIN_GO_VERSION = "1.19"
    MIN_PROTOC_VERSION = "3"

    def __init__(self, server, cert_path, key_path, cb_host="127.0.0.1",
                 port=CNG_DEFAULT_PORT):
        """
        @param server: TestInputServer object for the VM to run CNG on.
        @param cert_path: Absolute path to the TLS certificate on the VM.
        @param key_path: Absolute path to the TLS private key on the VM.
        @param cb_host: Couchbase Server host CNG should proxy to.
        @param port: Port CNG listens on (for readiness checks).
        """
        self.server = server
        self.cert_path = cert_path
        self.key_path = key_path
        self.cb_host = cb_host
        self.port = port
        self._pid = None
        self._limited_unit = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._limited_unit:
                self.stop_limited()
        except Exception as e:
            log.warning("stop_limited on __exit__ failed: {0}".format(e))
        try:
            self.cleanup()
        except Exception as e:
            log.warning("cleanup on __exit__ failed: {0}".format(e))
        return False

    @staticmethod
    def _first_nonempty(*streams):
        """Return the first non-empty stripped line across ordered streams.

        Guards against the common shell pattern where stdout is empty but
        stderr holds useful output (e.g. `go version` on some distros).
        """
        for stream in streams:
            if not stream:
                continue
            for line in stream:
                stripped = (line or "").strip()
                if stripped:
                    return stripped
        return ""

    # Directories worth probing when PATH discovery fails. Covers Homebrew
    # on Intel/ARM macOS, /usr/local installs, user-local pip/go installs,
    # and the $GOPATH/bin convention. The Go tarball install (official
    # install method on Linux) drops the `go` binary at /usr/local/go/bin
    # — that dir is NOT on the default non-interactive PATH, and
    # $HOME/go/bin is GOPATH/bin (third-party tools), not where `go`
    # itself lives. Add every common Go-install location so check
    # succeeds regardless of how the host installed Go.
    _EXTRA_PATH_DIRS = (
        "/usr/local/bin",
        "/usr/local/sbin",
        "/usr/local/go/bin",
        "/opt/go/bin",
        "/opt/homebrew/bin",
        "/opt/homebrew/sbin",
        "/snap/bin",
        "$HOME/.local/bin",
        "$HOME/go/bin",
        "$HOME/bin",
    )

    # Candidate absolute paths for the `go` binary, probed in order when
    # PATH-based discovery fails (e.g. Go installed somewhere we don't
    # list in _EXTRA_PATH_DIRS). First match wins.
    _GO_BINARY_CANDIDATES = (
        "/usr/local/go/bin/go",
        "/opt/go/bin/go",
        "/usr/lib/go/bin/go",
        "/snap/bin/go",
        "/opt/homebrew/bin/go",
        "/usr/local/bin/go",
    )

    @classmethod
    def _login_cmd(cls, cmd):
        """Wrap a command so it runs with the user's interactive PATH.

        `RemoteMachineShellConnection.execute_command` opens a non-login,
        non-interactive SSH channel. Three things conspire against PATH:

          * Non-interactive shells skip `/etc/profile` and `~/.bash_profile`.
          * The default `~/.bashrc` on Ubuntu/RHEL starts with
            `[[ $- != *i* ]] && return`, so sourcing it non-interactively
            aborts before any PATH exports.
          * Tools installed under `/usr/local/bin`, `~/.local/bin`, or
            `~/go/bin` (protoc, protoc-gen-go, brew-installed protoc) rely
            on those profile files being sourced.

        Workaround: source every profile-family file we can find, then
        unconditionally prepend well-known install directories to PATH so
        we never depend on the interactive guard being stepped over. The
        whole script is passed through base64 to sidestep shell quoting.
        """
        path_prefix = ":".join(cls._EXTRA_PATH_DIRS)
        script = (
            'set -o pipefail\n'
            'export PATH="{prefix}:$PATH"\n'
            '[ -r /etc/profile ] && . /etc/profile >/dev/null 2>&1\n'
            'for f in /etc/profile.d/*.sh; do '
            '[ -r "$f" ] && . "$f" >/dev/null 2>&1; '
            'done\n'
            '[ -r "$HOME/.bash_profile" ] && '
            '. "$HOME/.bash_profile" >/dev/null 2>&1\n'
            '[ -r "$HOME/.profile" ] && '
            '. "$HOME/.profile" >/dev/null 2>&1\n'
            '[ -r "$HOME/.bashrc" ] && '
            '. "$HOME/.bashrc" >/dev/null 2>&1\n'
            # Re-prepend in case a sourced file reset PATH.
            'export PATH="{prefix}:$PATH"\n'
            '{cmd}\n'
        ).format(prefix=path_prefix, cmd=cmd)
        encoded = base64.b64encode(script.encode('utf-8')).decode('ascii')
        return "printf %s {0} | base64 -d | bash".format(encoded)

    def _remote_path(self, shell):
        """Return the PATH the login-wrapped shell ends up seeing. Diagnostic."""
        output, _ = shell.execute_command(self._login_cmd("echo \"$PATH\""))
        return self._first_nonempty(output)

    def _resolve_go_path(self, shell):
        """Last-resort fallback when `go` isn't on the non-interactive PATH.

        Probes well-known absolute install paths on the remote host; the
        first existing+executable hit wins. Caches into self._go_path so
        install_and_build reuses it for `go generate` / `go build`
        without re-probing.
        """
        cached = getattr(self, "_go_path", None)
        if cached:
            return cached
        for candidate in self._GO_BINARY_CANDIDATES:
            out, _ = shell.execute_command(
                "test -x {0} && echo FOUND || true".format(candidate))
            if out and any("FOUND" in (line or "") for line in out):
                self._go_path = candidate
                log.info("Resolved go binary on {0}: {1}".format(
                    self.server.ip, candidate))
                return candidate
        self._go_path = None
        return None

    def _go_cmd(self, args):
        """Build a go command string using the resolved absolute path if
        available, otherwise plain `go` (so `_login_cmd`'s PATH munging
        still has a chance). Prepends the resolved binary's dirname to
        PATH so `go build` subprocesses (which shell out to `go tool
        compile` etc.) also find the toolchain.
        """
        go_path = getattr(self, "_go_path", None)
        if go_path:
            go_bin_dir = go_path.rsplit("/", 1)[0]
            return 'export PATH="{0}:$PATH"; "{1}" {2}'.format(
                go_bin_dir, go_path, args)
        return "go {0}".format(args)

    def check_dependencies(self):
        """Verify that Go and protoc are installed with required versions."""
        shell = RemoteMachineShellConnection(self.server)
        try:
            output, error = shell.execute_command(
                self._login_cmd("go version"))
            go_version = self._first_nonempty(output, error)
            if not go_version or "go version" not in go_version.lower():
                # PATH-based discovery failed. Probe known absolute paths
                # before giving up — the host may have `go` at a location
                # the non-interactive shell never sees.
                resolved = self._resolve_go_path(shell)
                if resolved:
                    output, error = shell.execute_command(
                        self._login_cmd('"{0}" version'.format(resolved)))
                    go_version = self._first_nonempty(output, error)
            if not go_version or "go version" not in go_version.lower():
                raise Exception(
                    "Go is not installed on {0} (stdout={1!r} stderr={2!r} "
                    "PATH={3!r}). Install Go v{4}+ first.".format(
                        self.server.ip, output, error,
                        self._remote_path(shell), self.MIN_GO_VERSION))
            log.info("Go version on {0}: {1}".format(self.server.ip, go_version))

            self._ensure_git_installed(shell)

            # protoc is only required when regenerating .pb.go stubs. The
            # stellar-gateway repo commits those stubs, so a regular build
            # succeeds without it. Warn if missing but do not fail — if
            # `go generate` later actually needs protoc, it will fail with
            # its own clear error.
            output, error = shell.execute_command(
                self._login_cmd("protoc --version"))
            protoc_version = self._first_nonempty(output, error)
            if protoc_version and "libprotoc" in protoc_version.lower():
                log.info("protoc version on {0}: {1}".format(
                    self.server.ip, protoc_version))
            else:
                log.warning(
                    "protoc not found on {0} (stdout={1!r} stderr={2!r}). "
                    "Continuing; a build will only need protoc if it has to "
                    "regenerate .pb.go stubs. Install protoc v{3}+ if "
                    "`go generate` later fails.".format(
                        self.server.ip, output, error, self.MIN_PROTOC_VERSION))

            output, _ = shell.execute_command(
                self._login_cmd("command -v protoc-gen-go"))
            if output and output[0].strip():
                log.info("protoc-gen-go found on {0}".format(self.server.ip))
            else:
                log.warning(
                    "protoc-gen-go not found on {0}. "
                    "Install with: go install google.golang.org/protobuf/cmd/protoc-gen-go@latest".format(
                        self.server.ip))

            output, _ = shell.execute_command(
                self._login_cmd("command -v protoc-gen-go-grpc"))
            if output and output[0].strip():
                log.info("protoc-gen-go-grpc found on {0}".format(self.server.ip))
            else:
                log.warning(
                    "protoc-gen-go-grpc not found on {0}. "
                    "Install with: go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest".format(
                        self.server.ip))

            log.info("Dependency check passed on {0}".format(self.server.ip))
        finally:
            shell.disconnect()

    # Distro -> (package-manager probe, install command). Ordered so that
    # the most specific manager wins on hosts where multiple are present
    # (e.g. dnf preferred over yum on RHEL9, apt-get over apt for
    # stable non-interactive output).
    _GIT_INSTALL_ROUTES = (
        ("apt-get",
         "(DEBIAN_FRONTEND=noninteractive apt-get update -y >/dev/null 2>&1 "
         "|| true) && DEBIAN_FRONTEND=noninteractive apt-get install -y "
         "--no-install-recommends git"),
        ("dnf", "dnf install -y git"),
        ("yum", "yum install -y git"),
        ("zypper", "zypper --non-interactive install git"),
        ("apk", "apk add --no-cache git"),
    )

    def _ensure_git_installed(self, shell):
        """Ensure `git` is available; install via the host's package manager
        if not. Minimal cloud images (notably debian-cloud and several
        rocky/alma minimal images) ship without git, so the clone step in
        `_ensure_clean_clone` would fail with rc=127 / "git: command not
        found". Detect a usable package manager and install
        non-interactively before that runs.
        """
        out, _ = shell.execute_command(self._login_cmd("command -v git"))
        if out and any((line or "").strip() for line in out):
            log.info("git found on {0}: {1}".format(
                self.server.ip, self._first_nonempty(out)))
            return

        log.warning(
            "git not found on {0}; attempting auto-install".format(
                self.server.ip))

        last_err = None
        installed = False
        for pkg_mgr, install_cmd in self._GIT_INSTALL_ROUTES:
            probe_out, _ = shell.execute_command(
                self._login_cmd("command -v {0}".format(pkg_mgr)))
            if not (probe_out and any(
                    (line or "").strip() for line in probe_out)):
                continue
            try:
                self._run_and_check(
                    shell, label="git install via {0}".format(pkg_mgr),
                    cmd=install_cmd, timeout=600)
                installed = True
                break
            except Exception as e:
                last_err = e
                log.warning(
                    "git install via {0} failed on {1}: {2}".format(
                        pkg_mgr, self.server.ip, e))

        if not installed:
            raise Exception(
                "Could not auto-install git on {0}: no usable package "
                "manager among apt-get/dnf/yum/zypper/apk, or all install "
                "attempts failed. Last error: {1}".format(
                    self.server.ip, last_err))

        out, _ = shell.execute_command(self._login_cmd("command -v git"))
        if not (out and any((line or "").strip() for line in out)):
            raise Exception(
                "git auto-install on {0} reported success but "
                "`command -v git` still empty (PATH={1!r}). Last "
                "install error: {2}".format(
                    self.server.ip, self._remote_path(shell), last_err))
        log.info("git installed on {0}: {1}".format(
            self.server.ip, self._first_nonempty(out)))

    def _run_and_check(self, shell, label, cmd, timeout=300):
        """Run `cmd` through _login_cmd and verify a non-zero exit didn't slip
        past stderr inspection. Append `; echo CNG_RC=$?` so the wrapped
        cmd's true exit lands in stdout regardless of whether the script
        exited via pipefail. Raise on RC != 0.
        """
        wrapped = "{0}; rc=$?; echo CNG_RC_{1}=$rc; exit $rc".format(
            cmd, label.replace(" ", "_").upper())
        output, error = shell.execute_command(
            self._login_cmd(wrapped), timeout=timeout)
        log.info("{0} output: {1}".format(label, output))
        if error:
            log.warning("{0} stderr: {1}".format(label, error))
        marker = "CNG_RC_{0}=".format(label.replace(" ", "_").upper())
        rc = None
        for line in output or []:
            if marker in line:
                try:
                    rc = int(line.split(marker, 1)[1].strip())
                except (ValueError, IndexError):
                    pass
                break
        if rc is None:
            raise Exception(
                "{0} did not produce an exit-code marker on {1}; "
                "stdout={2!r} stderr={3!r}".format(
                    label, self.server.ip, output, error))
        if rc != 0:
            raise Exception(
                "{0} failed on {1} with rc={2}; stdout={3!r} stderr={4!r}".format(
                    label, self.server.ip, rc, output, error))

    def install_and_build(self):
        """Clone stellar-gateway repo and build the binary on the remote VM.

        Resilient to partial clones left by earlier test runs on rebalanced
        nodes: a node that was rebalanced-out and then rebalanced-in to a
        later test commonly carries a half-populated /opt/cng/stellar-gateway
        from before. `test -d` would pass, `git pull` would fail silently
        in a tree without a remote, `go build` would produce nothing, and
        the binary-existence check would raise without saying why. Validate
        the working tree before pulling and force a re-clone if anything
        is missing.
        """
        self.check_dependencies()

        shell = RemoteMachineShellConnection(self.server)
        try:
            log.info("Installing stellar-gateway on {0}".format(self.server.ip))

            shell.execute_command("mkdir -p /opt/cng")

            try:
                self._ensure_clean_clone(shell)
                self._run_and_check(
                    shell, label="go generate",
                    cmd="cd {0} && {1}".format(
                        STELLAR_GATEWAY_INSTALL_DIR,
                        self._go_cmd("generate ./...")),
                    timeout=300)
                self._run_and_check(
                    shell, label="go build",
                    cmd="cd {0} && {1}".format(
                        STELLAR_GATEWAY_INSTALL_DIR,
                        self._go_cmd("build -o stellar-gateway ./cmd/gateway")),
                    timeout=300)
            except Exception:
                self._dump_install_diagnostics(shell)
                raise

            output, _ = shell.execute_command(
                "test -f {0} && echo 'OK' || echo 'FAIL'".format(
                    STELLAR_GATEWAY_BINARY))
            if output and output[0].strip() == "OK":
                log.info("stellar-gateway binary built successfully on {0}".format(
                    self.server.ip))
            else:
                self._dump_install_diagnostics(shell)
                raise Exception(
                    "stellar-gateway binary not found after build on {0}".format(
                        self.server.ip))
        finally:
            shell.disconnect()

    def _ensure_clean_clone(self, shell):
        """Validate the on-disk clone; re-clone if any required artefact is
        missing, then sync to origin/HEAD with hard reset so a previously
        dirty tree can't poison `go build`."""
        validate_cmd = (
            "if [ -d {dir}/.git ] && [ -f {dir}/go.mod ] "
            "&& [ -d {dir}/cmd/gateway ]; then echo VALID; "
            "else echo INVALID; fi"
        ).format(dir=STELLAR_GATEWAY_INSTALL_DIR)
        out, _ = shell.execute_command(validate_cmd)
        valid = bool(out) and any(line.strip() == "VALID" for line in out)
        if not valid:
            log.warning(
                "stellar-gateway clone at {0}:{1} is missing or partial; "
                "wiping and re-cloning. validate_output={2!r}".format(
                    self.server.ip, STELLAR_GATEWAY_INSTALL_DIR, out))
            shell.execute_command(
                "rm -rf {0}".format(STELLAR_GATEWAY_INSTALL_DIR))

        self._run_and_check(
            shell, label="git clone",
            cmd=("if [ ! -d {dir}/.git ]; then "
                 "rm -rf {dir} && git clone {repo} {dir}; "
                 "else echo 'clone present, skipping'; fi").format(
                dir=STELLAR_GATEWAY_INSTALL_DIR,
                repo=STELLAR_GATEWAY_REPO),
            timeout=300)

        # Fetch then hard-reset to origin/HEAD so local commits,
        # uncommitted changes, or rebase debris from a prior run can't
        # survive into the next build. Using origin/HEAD (set by clone)
        # rather than @{u} keeps this safe even on a detached HEAD.
        self._run_and_check(
            shell, label="git sync",
            cmd=("cd {dir} && git fetch --all --prune "
                 "&& git reset --hard origin/HEAD "
                 "&& git clean -fdx").format(
                dir=STELLAR_GATEWAY_INSTALL_DIR),
            timeout=300)

    def _dump_install_diagnostics(self, shell):
        """Best-effort diagnostic dump for install/build failures. Errors
        are swallowed — the original exception must propagate intact."""
        probes = [
            ("disk", "df -h /opt /tmp 2>&1 || true"),
            ("dir-listing",
             "ls -la {0} 2>&1 || true".format(STELLAR_GATEWAY_INSTALL_DIR)),
            ("git-status",
             "cd {0} 2>/dev/null && git status 2>&1; "
             "cd {0} 2>/dev/null && git log -1 --oneline 2>&1; "
             "true".format(STELLAR_GATEWAY_INSTALL_DIR)),
            ("go-env",
             self._login_cmd("{0} env GOPATH GOROOT GOOS GOARCH 2>&1; "
                             "{0} version 2>&1".format(
                                 getattr(self, "_go_path", None) or "go"))),
        ]
        for label, cmd in probes:
            try:
                out, err = shell.execute_command(cmd)
                log.error("[install-diag {0} on {1}] stdout={2!r} stderr={3!r}".format(
                    label, self.server.ip, out, err))
            except Exception as e:
                log.warning("[install-diag {0} on {1}] probe failed: {2}".format(
                    label, self.server.ip, e))

    def _wait_for_port(self, shell, on_failure_log_cmd, label):
        """Poll until self.port is in LISTEN state. Raise on timeout.

        Uses `ss` native filtering (sport = :<port>) so we never false-match
        a port number that happens to appear inside a PID or command column.
        """
        port_end = time.time() + PORT_READY_TIMEOUT
        grep_cmd = (
            "ss -Hltn 'sport = :{port}' 2>/dev/null | grep -q LISTEN "
            "&& echo READY || true").format(port=self.port)
        while time.time() < port_end:
            output, _ = shell.execute_command(grep_cmd)
            if output and any("READY" in line for line in output):
                log.info("{0} port {1} listening on {2}".format(
                    label, self.port, self.server.ip))
                return
            time.sleep(PORT_READY_INTERVAL)

        log_out, _ = shell.execute_command(on_failure_log_cmd)
        log.error("{0} logs: {1}".format(label, log_out))
        raise Exception(
            "{0} port {1} not listening on {2} after {3}s".format(
                label, self.port, self.server.ip, PORT_READY_TIMEOUT))

    def _kill_tracked_pid(self, shell, signal=""):
        """Kill self._pid if set. No-op if we haven't tracked a launch."""
        if not self._pid:
            return False
        flag = "-{0} ".format(signal) if signal else ""
        shell.execute_command("kill {0}{1} 2>/dev/null || true".format(
            flag, self._pid))
        return True

    def _wait_for_no_cng_processes(self, shell, timeout=10):
        """Poll pgrep -f stellar-gateway until no instance remains, capped
        at `timeout` seconds. Escalates to SIGKILL once if SIGTERM didn't
        take, then polls a final second.

        Replaces a fixed sleep after pkill — on a loaded VM signal
        delivery + process exit can exceed any single sleep, and a
        subsequent launch races the dying process for the listening
        port and on-disk locks.
        """
        deadline = time.time() + timeout
        while time.time() < deadline:
            out, _ = shell.execute_command(
                "pgrep -f stellar-gateway || true")
            if not (out and any(line.strip() for line in out)):
                return
            time.sleep(1)
        log.warning(
            "stellar-gateway still alive {0}s after kill on {1}; "
            "escalating to SIGKILL".format(timeout, self.server.ip))
        shell.execute_command("pkill -9 -f stellar-gateway || true")
        time.sleep(1)
        out, _ = shell.execute_command("pgrep -f stellar-gateway || true")
        if out and any(line.strip() for line in out):
            log.warning(
                "stellar-gateway processes survived SIGKILL on {0} "
                "(pids={1}); proceeding anyway — downstream liveness "
                "check will catch a port collision".format(
                    self.server.ip, out))

    def start(self):
        """Start stellar-gateway as a background process and verify port readiness."""
        shell = RemoteMachineShellConnection(self.server)
        try:
            log.info("Starting stellar-gateway on {0}".format(self.server.ip))

            # Fresh start: kill any CNG this helper previously tracked; do not
            # touch other instances on the host. Callers running multiple
            # helpers on the same host must coordinate externally.
            if not self._kill_tracked_pid(shell):
                # No tracked PID — one-time sweep handles stale processes from
                # a crashed prior run.
                shell.execute_command("pkill -f stellar-gateway || true")            
            self._wait_for_no_cng_processes(shell, timeout=10)

            # Capture $! from the launching shell — pgrep -f after the fact
            # would race with any stale stellar-gateway from a prior crash
            # and lock onto the wrong PID. echo CNG_PID=$! is the only way
            # to know which PID this nohup actually spawned.
            cmd = ("nohup {binary} --cert {cert} --key {key} "
                   "--cb-host {cb_host} "
                   "> /opt/cng/stellar-gateway.log 2>&1 & "
                   "echo CNG_PID=$!").format(
                binary=STELLAR_GATEWAY_BINARY,
                cert=self.cert_path,
                key=self.key_path,
                cb_host=self.cb_host)
            output, _ = shell.execute_command(cmd)
            pid = None
            for line in output or []:
                if "CNG_PID=" in line:
                    pid = line.split("CNG_PID=", 1)[1].strip()
                    break
            if not pid or not pid.isdigit():
                log_out, _ = shell.execute_command(
                    "tail -20 /opt/cng/stellar-gateway.log")
                log.error("stellar-gateway logs: {0}".format(log_out))
                raise Exception(
                    "stellar-gateway launch did not return a PID on {0}; "
                    "output={1!r}".format(self.server.ip, output))

            # Liveness window: nohup may exit immediately if the binary
            # mis-execs. Poll kill -0 instead of pgrep so we verify the
            # exact PID we launched is still up.
            end_time = time.time() + 15
            alive = False
            while time.time() < end_time:
                check, _ = shell.execute_command(
                    "kill -0 {0} 2>/dev/null && echo ALIVE || true".format(pid))
                if check and any("ALIVE" in line for line in check):
                    alive = True
                    break
                time.sleep(2)
            if not alive:
                log_out, _ = shell.execute_command(
                    "tail -20 /opt/cng/stellar-gateway.log")
                log.error("stellar-gateway logs: {0}".format(log_out))
                raise Exception(
                    "stellar-gateway PID {0} died immediately after launch "
                    "on {1}".format(pid, self.server.ip))
            self._pid = pid
            log.info("stellar-gateway started with PID {0} on {1}".format(
                pid, self.server.ip))

            self._wait_for_port(
                shell, "tail -20 /opt/cng/stellar-gateway.log", "CNG")
        finally:
            shell.disconnect()

    def stop(self):
        """Stop stellar-gateway on the remote VM."""
        shell = RemoteMachineShellConnection(self.server)
        try:
            log.info("Stopping stellar-gateway on {0}".format(self.server.ip))
            # Prefer killing the PID we started. pkill fallback only when we
            # never tracked one (helper was never started in this process).
            killed_by_pid = self._kill_tracked_pid(shell)
            if not killed_by_pid:
                shell.execute_command("pkill -f stellar-gateway || true")

            end_time = time.time() + 10
            while time.time() < end_time:
                if self._pid:
                    output, _ = shell.execute_command(
                        "kill -0 {0} 2>/dev/null && echo ALIVE || true".format(
                            self._pid))
                    alive = output and any("ALIVE" in l for l in output)
                else:
                    output, _ = shell.execute_command("pgrep -f stellar-gateway")
                    alive = bool(output)
                if not alive:
                    log.info("stellar-gateway stopped on {0}".format(
                        self.server.ip))
                    self._pid = None
                    return
                time.sleep(2)

            # SIGKILL: scoped to tracked PID if we have one.
            if not self._kill_tracked_pid(shell, signal="9"):
                shell.execute_command("pkill -9 -f stellar-gateway || true")
            time.sleep(1)
            log.info("stellar-gateway force-stopped on {0}".format(
                self.server.ip))
            self._pid = None
        finally:
            shell.disconnect()

    def cleanup(self):
        """Stop CNG and clean up logs in a single SSH session."""
        shell = RemoteMachineShellConnection(self.server)
        try:
            log.info("Cleaning up CNG on {0}".format(self.server.ip))
            killed_by_pid = self._kill_tracked_pid(shell)
            if not killed_by_pid:
                shell.execute_command("pkill -f stellar-gateway || true")

            end_time = time.time() + 10
            still_alive = True
            while time.time() < end_time:
                if self._pid:
                    output, _ = shell.execute_command(
                        "kill -0 {0} 2>/dev/null && echo ALIVE || true".format(
                            self._pid))
                    still_alive = bool(
                        output and any("ALIVE" in l for l in output))
                else:
                    output, _ = shell.execute_command("pgrep -f stellar-gateway")
                    still_alive = bool(output)
                if not still_alive:
                    break
                time.sleep(2)

            if still_alive:
                if not self._kill_tracked_pid(shell, signal="9"):
                    shell.execute_command("pkill -9 -f stellar-gateway || true")
                time.sleep(1)

            shell.execute_command("rm -rf /opt/cng/stellar-gateway.log")
            self._pid = None
            log.info("CNG cleanup done on {0}".format(self.server.ip))
        finally:
            shell.disconnect()

    def is_running(self):
        """Check if stellar-gateway is running on the remote VM."""
        shell = RemoteMachineShellConnection(self.server)
        try:
            if self._pid:
                output, _ = shell.execute_command(
                    "kill -0 {0} 2>/dev/null && echo ALIVE || true".format(
                        self._pid))
                return bool(output and any("ALIVE" in l for l in output))
            output, _ = shell.execute_command("pgrep -f stellar-gateway")
            return bool(output)
        finally:
            shell.disconnect()

    def start_with_limits(self, cpu_quota="15%", memory_max="256M",
                          unit_name="cng-limited"):
        """Start stellar-gateway under systemd-run with CPU and memory caps.

        Used to simulate a resource-constrained CNG pod for flow-control tests.
        Requires systemd; swap is disabled for the unit so MemoryMax is hard.

        Process lifecycle is scoped to the systemd unit — we never pkill by
        name, so a concurrent non-limited CNG on the host is unaffected.

        @param cpu_quota: CPUQuota value (e.g. "15%", "50%").
        @param memory_max: MemoryMax value (e.g. "128M", "512M").
        @param unit_name: Transient systemd unit name.
        """
        if not _UNIT_NAME_RE.match(unit_name):
            raise ValueError(
                "Invalid systemd unit_name {0!r}; must match {1}".format(
                    unit_name, _UNIT_NAME_RE.pattern))
        if not _CPU_QUOTA_RE.match(cpu_quota):
            raise ValueError(
                "Invalid cpu_quota {0!r}; expected e.g. '15%'".format(cpu_quota))
        if not _MEMORY_MAX_RE.match(memory_max):
            raise ValueError(
                "Invalid memory_max {0!r}; expected e.g. '256M'".format(
                    memory_max))

        shell = RemoteMachineShellConnection(self.server)
        try:
            log.info("Starting stellar-gateway on {0} with CPUQuota={1}, "
                     "MemoryMax={2} (unit={3})".format(
                        self.server.ip, cpu_quota, memory_max, unit_name))

            # Stop any prior incarnation of THIS unit only.
            shell.execute_command(
                "systemctl stop '{0}' 2>/dev/null || true".format(unit_name))
            time.sleep(2)

            # Set unit tracking before launching so a failure mid-start still
            # lets stop_limited() target the correct unit.
            self._limited_unit = unit_name

            cmd = ("systemd-run --unit='{unit}' "
                   "-p CPUQuota='{cpu}' -p MemoryMax='{mem}' "
                   "-p MemorySwapMax=0 "
                   "'{binary}' --cert '{cert}' --key '{key}' "
                   "--cb-host '{cb_host}'").format(
                unit=unit_name, cpu=cpu_quota, mem=memory_max,
                binary=STELLAR_GATEWAY_BINARY,
                cert=self.cert_path, key=self.key_path,
                cb_host=self.cb_host)
            output, error = shell.execute_command(cmd)
            if error and any("Failed" in e or "not found" in e.lower()
                             for e in error):
                raise Exception(
                    "systemd-run failed on {0}: {1}".format(self.server.ip, error))

            self._wait_for_port(
                shell,
                "journalctl -u '{0}' -n 30 --no-pager || true".format(unit_name),
                "CNG (limited cpu={0}, mem={1})".format(cpu_quota, memory_max))
        finally:
            shell.disconnect()

    def stop_limited(self):
        """Stop the transient limited CNG unit started via start_with_limits.

        Scoped to the systemd unit; leaves any other CNG on the host intact.
        """
        shell = RemoteMachineShellConnection(self.server)
        try:
            unit = self._limited_unit
            if not unit:
                log.info(
                    "stop_limited() called with no tracked unit on {0}; "
                    "nothing to do".format(self.server.ip))
                return
            shell.execute_command(
                "systemctl stop '{0}' 2>/dev/null || true".format(unit))
            log.info("Stopped limited CNG unit '{0}' on {1}".format(
                unit, self.server.ip))
            self._limited_unit = None
        finally:
            shell.disconnect()
