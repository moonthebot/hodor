"""
MCP Server — core gRPC implementation
"""

from __future__ import annotations

import asyncio
import logging
import os
import shlex
import subprocess
import threading
import time
import uuid
from concurrent import futures
from datetime import datetime, timezone
from typing import Any

import grpc
import yaml
from google.protobuf.timestamp_pb2 import Timestamp

import mcp_pb2
import mcp_pb2_grpc

log = logging.getLogger("mcp_server")

VERSION = "0.1.0"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ts_now() -> Timestamp:
    t = Timestamp()
    t.GetCurrentTime()
    return t


def ts_from_unix(unix: float) -> Timestamp:
    t = Timestamp()
    t.seconds = int(unix)
    t.nanos = int((unix - int(unix)) * 1e9)
    return t


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class Config:
    def __init__(self, path: str):
        with open(path) as f:
            data = yaml.safe_load(f)
        self.host: str = data["server"]["host"]
        self.port: int = data["server"]["port"]
        self.bearer_token: str = data["server"]["bearer_token"]

        # Map alias/hostname -> host config dict
        self.hosts: dict[str, dict] = {}
        for h in data.get("hosts", []):
            self.hosts[h["hostname"]] = h
            if h.get("alias"):
                self.hosts[h["alias"]] = h

        self.command_allowlist: set[str] = set(data.get("command_allowlist", []))


# ---------------------------------------------------------------------------
# In-memory job store
# ---------------------------------------------------------------------------

class Job:
    def __init__(self, job_id: str, kind: int, target: str):
        self.job_id = job_id
        self.kind = kind
        self.target = target
        self.status = mcp_pb2.JOB_PENDING
        self.exit_code: int = 0
        self.proxy_error: str = ""
        self.stdout_buf: list[bytes] = []
        self.stderr_buf: list[bytes] = []
        self.log_events: list[mcp_pb2.JobEvent] = []
        self.submitted_at = ts_now()
        self.started_at: Timestamp | None = None
        self.finished_at: Timestamp | None = None
        self._waiters: list[asyncio.Queue] = []
        self._lock = threading.Lock()

    def add_log(self, stream: int, data: bytes):
        evt = mcp_pb2.JobEvent(
            job_id=self.job_id,
            log=mcp_pb2.LogChunk(stream=stream, data=data, ts=ts_now()),
        )
        with self._lock:
            self.log_events.append(evt)
            if stream == mcp_pb2.STDOUT:
                self.stdout_buf.append(data)
            else:
                self.stderr_buf.append(data)
        for q in self._waiters:
            try:
                q.put_nowait(evt)
            except Exception:
                pass

    def set_status(self, status: int, exit_code: int = 0, proxy_error: str = ""):
        evt = mcp_pb2.JobEvent(
            job_id=self.job_id,
            status_change=mcp_pb2.JobStatusChange(
                new_status=status,
                exit_code=exit_code,
                proxy_error=proxy_error,
                ts=ts_now(),
            ),
        )
        with self._lock:
            self.status = status
            self.exit_code = exit_code
            self.proxy_error = proxy_error
            self.log_events.append(evt)
            if status == mcp_pb2.JOB_RUNNING:
                self.started_at = ts_now()
            elif status in (
                mcp_pb2.JOB_SUCCEEDED,
                mcp_pb2.JOB_FAILED,
                mcp_pb2.JOB_TIMED_OUT,
                mcp_pb2.JOB_CANCELLED,
            ):
                self.finished_at = ts_now()
        for q in self._waiters:
            try:
                q.put_nowait(evt)
            except Exception:
                pass
        # sentinel for terminal status
        if status not in (mcp_pb2.JOB_PENDING, mcp_pb2.JOB_RUNNING):
            for q in self._waiters:
                try:
                    q.put_nowait(None)
                except Exception:
                    pass

    def is_terminal(self) -> bool:
        return self.status not in (mcp_pb2.JOB_PENDING, mcp_pb2.JOB_RUNNING)

    def output_tail(self, n: int = 2048) -> str:
        combined = b"".join(self.stdout_buf + self.stderr_buf)
        return combined[-n:].decode(errors="replace")


jobs: dict[str, Job] = {}
jobs_lock = threading.Lock()


def new_job(kind: int, target: str) -> Job:
    jid = f"j_{uuid.uuid4().hex[:8]}"
    job = Job(jid, kind, target)
    with jobs_lock:
        jobs[jid] = job
    return job


# ---------------------------------------------------------------------------
# Command execution
# ---------------------------------------------------------------------------

def execute_local(
    command: str,
    args: list[str],
    workdir: str | None,
    env_extra: dict[str, str],
    timeout: int,
    job: Job | None = None,
) -> tuple[str, str, int, str]:
    """Run a command as a local subprocess. Returns (stdout, stderr, exit_code, proxy_error)."""
    cmd = [command] + args
    env = os.environ.copy()
    env.update(env_extra)
    cwd = workdir or None

    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            cwd=cwd,
        )

        if job:
            job.set_status(mcp_pb2.JOB_RUNNING)

        stdout_chunks = []
        stderr_chunks = []

        def read_stream(stream, chunks, stream_id):
            for line in iter(stream.readline, b""):
                chunks.append(line)
                if job:
                    job.add_log(stream_id, line)
            stream.close()

        t_out = threading.Thread(target=read_stream, args=(proc.stdout, stdout_chunks, mcp_pb2.STDOUT))
        t_err = threading.Thread(target=read_stream, args=(proc.stderr, stderr_chunks, mcp_pb2.STDERR))
        t_out.start()
        t_err.start()

        try:
            proc.wait(timeout=timeout or 60)
        except subprocess.TimeoutExpired:
            proc.kill()
            t_out.join(1)
            t_err.join(1)
            if job:
                job.set_status(mcp_pb2.JOB_TIMED_OUT, proxy_error="timeout exceeded")
            return "", "", -1, "timeout exceeded"

        t_out.join()
        t_err.join()

        stdout = b"".join(stdout_chunks).decode(errors="replace")
        stderr = b"".join(stderr_chunks).decode(errors="replace")
        return stdout, stderr, proc.returncode, ""

    except FileNotFoundError:
        proxy_err = f"command not found: {command}"
        if job:
            job.set_status(mcp_pb2.JOB_FAILED, proxy_error=proxy_err)
        return "", "", -1, proxy_err
    except Exception as e:
        proxy_err = str(e)
        if job:
            job.set_status(mcp_pb2.JOB_FAILED, proxy_error=proxy_err)
        return "", "", -1, proxy_err


def execute_ssh(
    host_cfg: dict,
    command: str,
    args: list[str],
    workdir: str | None,
    env_extra: dict[str, str],
    timeout: int,
    job: Job | None = None,
) -> tuple[str, str, int, str]:
    """Run a command over SSH. Returns (stdout, stderr, exit_code, proxy_error)."""
    try:
        import paramiko
    except ImportError:
        return "", "", -1, "paramiko not installed"

    address = host_cfg["address"]
    user = host_cfg.get("ssh_user", "ci")
    key_path = os.path.expanduser(host_cfg.get("ssh_key", "~/.ssh/id_rsa"))

    cmd_parts = []
    if workdir:
        cmd_parts.append(f"cd {shlex.quote(workdir)} &&")
    for k, v in env_extra.items():
        cmd_parts.append(f"export {shlex.quote(k)}={shlex.quote(v)};")
    cmd_parts.append(shlex.quote(command))
    cmd_parts.extend(shlex.quote(a) for a in args)
    full_cmd = " ".join(cmd_parts)

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        client.connect(
            address,
            username=user,
            key_filename=key_path if os.path.exists(key_path) else None,
            timeout=10,
        )
    except Exception as e:
        proxy_err = f"SSH connect failed: {e}"
        if job:
            job.set_status(mcp_pb2.JOB_FAILED, proxy_error=proxy_err)
        return "", "", -1, proxy_err

    if job:
        job.set_status(mcp_pb2.JOB_RUNNING)

    try:
        _, stdout_ch, stderr_ch = client.exec_command(full_cmd, timeout=timeout or 60)
        stdout = stdout_ch.read().decode(errors="replace")
        stderr = stderr_ch.read().decode(errors="replace")
        exit_code = stdout_ch.channel.recv_exit_status()

        if job:
            if stdout:
                job.add_log(mcp_pb2.STDOUT, stdout.encode())
            if stderr:
                job.add_log(mcp_pb2.STDERR, stderr.encode())

        return stdout, stderr, exit_code, ""
    except Exception as e:
        proxy_err = str(e)
        if job:
            job.set_status(mcp_pb2.JOB_FAILED, proxy_error=proxy_err)
        return "", "", -1, proxy_err
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Auth interceptor
# ---------------------------------------------------------------------------

class AuthInterceptor(grpc.ServerInterceptor):
    def __init__(self, token: str):
        self._token = token
        self._open_suffixes = {"/HealthCheck"}

    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method
        if any(method.endswith(s) for s in self._open_suffixes):
            return continuation(handler_call_details)

        metadata = dict(handler_call_details.invocation_metadata)
        auth = metadata.get("authorization", "")
        if auth == f"Bearer {self._token}":
            return continuation(handler_call_details)

        def abort(request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "invalid or missing bearer token")

        return grpc.unary_unary_rpc_method_handler(abort)


# ---------------------------------------------------------------------------
# gRPC servicer
# ---------------------------------------------------------------------------

class MCPServicer(mcp_pb2_grpc.MachineControlProxyServicer):
    def __init__(self, cfg: Config):
        self.cfg = cfg
        # reservation_id -> dict with state, ip, etc.
        self._reservations: dict[str, dict] = {}
        self._res_lock = threading.Lock()

    # ---- Auth helper ----

    def _resolve_host(self, name: str, context: grpc.ServicerContext) -> dict | None:
        host = self.cfg.hosts.get(name)
        if not host:
            context.abort(grpc.StatusCode.NOT_FOUND, f"unknown host: {name}")
            return None
        return host

    # ---- RunCommand ----

    def RunCommand(self, request: mcp_pb2.RunCommandRequest, context):
        # Validate command against allowlist
        if request.command not in self.cfg.command_allowlist:
            context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f"command '{request.command}' not in allowlist",
            )
            return mcp_pb2.RunCommandResponse()

        host = self._resolve_host(request.target_host, context)
        if host is None:
            return mcp_pb2.RunCommandResponse()

        env_extra = dict(request.env)
        timeout = request.timeout_seconds or 60

        if host.get("use_ssh", False):
            stdout, stderr, exit_code, proxy_error = execute_ssh(
                host, request.command, list(request.args),
                request.workdir or None, env_extra, timeout,
            )
        else:
            stdout, stderr, exit_code, proxy_error = execute_local(
                request.command, list(request.args),
                request.workdir or None, env_extra, timeout,
            )

        return mcp_pb2.RunCommandResponse(
            stdout=stdout,
            stderr=stderr,
            exit_code=exit_code,
            proxy_error=proxy_error,
        )

    # ---- RunBuild ----

    def RunBuild(self, request: mcp_pb2.RunBuildRequest, context):
        target_field = request.WhichOneof("target")
        target_name = getattr(request, target_field) if target_field else ""

        host_name = target_name
        if target_field == "reservation_id":
            with self._res_lock:
                res = self._reservations.get(target_name, {})
            host_name = res.get("hostname", target_name)

        job = new_job(mcp_pb2.BUILD, host_name)

        def run():
            host = self.cfg.hosts.get(host_name)
            cmd = request.build_command or "make"
            args = list(request.build_args)
            env = dict(request.env)
            timeout = request.timeout_seconds or 300
            workdir = request.workdir or None

            if host is None:
                job.set_status(mcp_pb2.JOB_FAILED, proxy_error=f"unknown host: {host_name}")
                return

            if host.get("use_ssh", False):
                _, _, rc, proxy_err = execute_ssh(host, cmd, args, workdir, env, timeout, job)
            else:
                _, _, rc, proxy_err = execute_local(cmd, args, workdir, env, timeout, job)

            if proxy_err:
                job.set_status(mcp_pb2.JOB_FAILED, exit_code=rc, proxy_error=proxy_err)
            elif rc == 0:
                job.set_status(mcp_pb2.JOB_SUCCEEDED, exit_code=0)
            else:
                job.set_status(mcp_pb2.JOB_FAILED, exit_code=rc)

        threading.Thread(target=run, daemon=True).start()
        return mcp_pb2.JobHandle(job_id=job.job_id, submitted_at=job.submitted_at)

    # ---- RunTests ----

    def RunTests(self, request: mcp_pb2.RunTestsRequest, context):
        target_field = request.WhichOneof("target")
        target_name = getattr(request, target_field) if target_field else ""

        host_name = target_name
        if target_field == "reservation_id":
            with self._res_lock:
                res = self._reservations.get(target_name, {})
            host_name = res.get("hostname", target_name)

        job = new_job(mcp_pb2.TEST, host_name)

        def run():
            host = self.cfg.hosts.get(host_name)
            cmd = request.runner or "pytest"
            args = list(request.test_paths) + list(request.runner_args)
            env = dict(request.env)
            timeout = request.timeout_seconds or 600
            workdir = request.workdir or None

            if host is None:
                job.set_status(mcp_pb2.JOB_FAILED, proxy_error=f"unknown host: {host_name}")
                return

            if host.get("use_ssh", False):
                _, _, rc, proxy_err = execute_ssh(host, cmd, args, workdir, env, timeout, job)
            else:
                _, _, rc, proxy_err = execute_local(cmd, args, workdir, env, timeout, job)

            if proxy_err:
                job.set_status(mcp_pb2.JOB_FAILED, exit_code=rc, proxy_error=proxy_err)
            elif rc == 0:
                job.set_status(mcp_pb2.JOB_SUCCEEDED, exit_code=0)
            else:
                job.set_status(mcp_pb2.JOB_FAILED, exit_code=rc)

        threading.Thread(target=run, daemon=True).start()
        return mcp_pb2.JobHandle(job_id=job.job_id, submitted_at=job.submitted_at)

    # ---- GetJob ----

    def GetJob(self, request: mcp_pb2.GetJobRequest, context):
        with jobs_lock:
            job = jobs.get(request.job_id)
        if not job:
            context.abort(grpc.StatusCode.NOT_FOUND, f"job not found: {request.job_id}")
            return mcp_pb2.GetJobResponse()
        return mcp_pb2.GetJobResponse(
            job_id=job.job_id,
            status=job.status,
            exit_code=job.exit_code,
            proxy_error=job.proxy_error,
            submitted_at=job.submitted_at,
            started_at=job.started_at or Timestamp(),
            finished_at=job.finished_at or Timestamp(),
            output_tail=job.output_tail(),
            kind=job.kind,
        )

    # ---- WatchJob ----

    def WatchJob(self, request: mcp_pb2.WatchJobRequest, context):
        with jobs_lock:
            job = jobs.get(request.job_id)
        if not job:
            context.abort(grpc.StatusCode.NOT_FOUND, f"job not found: {request.job_id}")
            return

        q: asyncio.Queue = asyncio.Queue() if False else __import__("queue").Queue()

        with job._lock:
            if request.replay_from_start:
                for evt in job.log_events:
                    yield evt
            terminal = job.is_terminal()

        if terminal:
            return

        job._waiters.append(q)
        try:
            while context.is_active():
                try:
                    evt = q.get(timeout=1.0)
                    if evt is None:
                        break
                    yield evt
                except __import__("queue").Empty:
                    continue
        finally:
            job._waiters.remove(q)

    # ---- CancelJob ----

    def CancelJob(self, request: mcp_pb2.CancelJobRequest, context):
        with jobs_lock:
            job = jobs.get(request.job_id)
        if not job:
            context.abort(grpc.StatusCode.NOT_FOUND, f"job not found: {request.job_id}")
            return mcp_pb2.CancelJobResponse()
        if job.is_terminal():
            return mcp_pb2.CancelJobResponse(accepted=False)
        job.set_status(mcp_pb2.JOB_CANCELLED)
        return mcp_pb2.CancelJobResponse(accepted=True)

    # ---- ReserveMachine (stub — real testflinger integration TBD) ----

    def ReserveMachine(self, request: mcp_pb2.ReserveMachineRequest, context):
        res_id = f"r_{uuid.uuid4().hex[:8]}"
        tf_job_id = f"tf_{uuid.uuid4().hex[:8]}"
        with self._res_lock:
            self._reservations[res_id] = {
                "state": mcp_pb2.PROVISIONING,
                "queue": request.queue,
                "label": request.label,
                "submitted_at": time.time(),
                "ip_address": "",
                "hostname": "",
                "failure_reason": "",
                "testflinger_job_id": tf_job_id,
            }
        # Simulate async provisioning (for tests, resolve quickly if queue=="loopback")
        def _provision():
            time.sleep(0.5)
            with self._res_lock:
                res = self._reservations[res_id]
                if request.queue == "loopback":
                    res["state"] = mcp_pb2.READY
                    res["ip_address"] = "127.0.0.1"
                    res["hostname"] = "loopback"
                else:
                    res["state"] = mcp_pb2.READY
                    res["ip_address"] = "10.8.0.99"
                    res["hostname"] = f"lab-{request.queue}-001"
        threading.Thread(target=_provision, daemon=True).start()

        return mcp_pb2.ReserveMachineResponse(
            reservation_id=res_id,
            testflinger_job_id=tf_job_id,
            submitted_at=ts_now(),
        )

    def GetReservation(self, request: mcp_pb2.GetReservationRequest, context):
        with self._res_lock:
            res = self._reservations.get(request.reservation_id)
        if not res:
            context.abort(grpc.StatusCode.NOT_FOUND, f"reservation not found: {request.reservation_id}")
            return mcp_pb2.GetReservationResponse()
        return mcp_pb2.GetReservationResponse(
            reservation_id=request.reservation_id,
            state=res["state"],
            ip_address=res.get("ip_address", ""),
            hostname=res.get("hostname", ""),
            failure_reason=res.get("failure_reason", ""),
            submitted_at=ts_from_unix(res["submitted_at"]),
        )

    def WatchReservation(self, request: mcp_pb2.WatchReservationRequest, context):
        import queue as q_mod
        q = q_mod.Queue()
        deadline = time.time() + 120

        while context.is_active() and time.time() < deadline:
            with self._res_lock:
                res = self._reservations.get(request.reservation_id)
            if not res:
                context.abort(grpc.StatusCode.NOT_FOUND, f"reservation not found: {request.reservation_id}")
                return
            evt = mcp_pb2.ReservationEvent(
                reservation_id=request.reservation_id,
                state=res["state"],
                ip_address=res.get("ip_address", ""),
                hostname=res.get("hostname", ""),
                failure_reason=res.get("failure_reason", ""),
                message=f"state={res['state']}",
                ts=ts_now(),
            )
            yield evt
            if res["state"] in (mcp_pb2.READY, mcp_pb2.FAILED, mcp_pb2.RELEASED):
                return
            time.sleep(0.5)

    def ReleaseMachine(self, request: mcp_pb2.ReleaseMachineRequest, context):
        with self._res_lock:
            res = self._reservations.get(request.reservation_id)
            if not res:
                return mcp_pb2.ReleaseMachineResponse(accepted=False)
            if res["state"] == mcp_pb2.RELEASED:
                return mcp_pb2.ReleaseMachineResponse(accepted=False)
            res["state"] = mcp_pb2.RELEASED
        return mcp_pb2.ReleaseMachineResponse(accepted=True)

    def ListReservations(self, request: mcp_pb2.ListReservationsRequest, context):
        with self._res_lock:
            items = list(self._reservations.items())
        results = []
        for res_id, res in items:
            if request.state_filter and res["state"] != request.state_filter:
                continue
            results.append(mcp_pb2.GetReservationResponse(
                reservation_id=res_id,
                state=res["state"],
                ip_address=res.get("ip_address", ""),
                hostname=res.get("hostname", ""),
                failure_reason=res.get("failure_reason", ""),
                submitted_at=ts_from_unix(res["submitted_at"]),
            ))
        return mcp_pb2.ListReservationsResponse(reservations=results)

    def ListHosts(self, request: mcp_pb2.ListHostsRequest, context):
        hosts = []
        seen = set()
        for name, h in self.cfg.hosts.items():
            hostname = h["hostname"]
            if hostname in seen:
                continue
            seen.add(hostname)
            if request.filter and request.filter.lower() not in hostname.lower():
                continue
            hosts.append(mcp_pb2.HostInfo(
                hostname=hostname,
                alias=h.get("alias", ""),
                reachable=True,
            ))
        return mcp_pb2.ListHostsResponse(hosts=hosts)

    def HealthCheck(self, request: mcp_pb2.HealthCheckRequest, context):
        return mcp_pb2.HealthCheckResponse(healthy=True, version=VERSION)


# ---------------------------------------------------------------------------
# Server entrypoint
# ---------------------------------------------------------------------------

def serve(config_path: str = "config.yaml"):
    cfg = Config(config_path)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    interceptors = [AuthInterceptor(cfg.bearer_token)]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), interceptors=interceptors)
    mcp_pb2_grpc.add_MachineControlProxyServicer_to_server(MCPServicer(cfg), server)

    addr = f"{cfg.host}:{cfg.port}"
    server.add_insecure_port(addr)
    server.start()
    log.info("MCP server listening on %s", addr)

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    import sys
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    serve(cfg_path)
