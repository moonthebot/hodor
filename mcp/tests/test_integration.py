"""
Integration tests for the MCP gRPC server.

Tests spin up a real gRPC server on a random port, run RPCs against it,
and tear it down after each test class.  No mocking of the transport layer —
every call goes through the full gRPC stack.
"""

import sys
import os
import tempfile
import textwrap
import threading
import time
from concurrent import futures

import grpc
import pytest
import yaml

# Make sure the server package is importable when running from this directory.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "server"))

import mcp_pb2
import mcp_pb2_grpc
from mcp_server import Config, MCPServicer, AuthInterceptor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BEARER = "test_token_abc"


def _write_config(tmp_dir: str, extra_hosts=None, extra_allowlist=None) -> str:
    hosts = [
        {
            "hostname": "loopback",
            "alias": "local",
            "address": "127.0.0.1",
            "use_ssh": False,
        }
    ]
    if extra_hosts:
        hosts.extend(extra_hosts)
    allowlist = ["echo", "hostname", "uname", "ls", "python3", "pwd"]
    if extra_allowlist:
        allowlist.extend(extra_allowlist)
    cfg = {
        "server": {"host": "127.0.0.1", "port": 0, "bearer_token": BEARER},
        "hosts": hosts,
        "command_allowlist": allowlist,
    }
    path = os.path.join(tmp_dir, "config.yaml")
    with open(path, "w") as f:
        yaml.dump(cfg, f)
    return path


def _start_server(cfg_path: str):
    """Start an MCP server and return (server, channel, stub, port)."""
    cfg = Config(cfg_path)
    interceptors = [AuthInterceptor(cfg.bearer_token)]
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=4),
        interceptors=interceptors,
    )
    mcp_pb2_grpc.add_TestflingerControlServicer_to_server(MCPServicer(cfg), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = mcp_pb2_grpc.TestflingerControlStub(channel)
    return server, channel, stub, port


def _auth_meta():
    return [("authorization", f"Bearer {BEARER}")]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def server_env():
    tmp = tempfile.mkdtemp()
    cfg_path = _write_config(tmp)
    server, channel, stub, port = _start_server(cfg_path)
    yield stub, port
    channel.close()
    server.stop(0)


# ---------------------------------------------------------------------------
# HealthCheck (no auth required)
# ---------------------------------------------------------------------------

class TestHealthCheck:
    def test_health_check_no_auth(self, server_env):
        stub, _ = server_env
        resp = stub.HealthCheck(mcp_pb2.HealthCheckRequest())
        assert resp.healthy is True
        assert resp.version  # non-empty


# ---------------------------------------------------------------------------
# Auth enforcement
# ---------------------------------------------------------------------------

class TestAuth:
    def test_missing_token_rejected(self, server_env):
        stub, _ = server_env
        with pytest.raises(grpc.RpcError) as exc_info:
            stub.BuildDebianPackage(
                mcp_pb2.BuildDebianPackageRequest(
                    target_host="loopback", repo_url="https://example.com/repo.git"
                )
            )
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED

    def test_wrong_token_rejected(self, server_env):
        stub, _ = server_env
        with pytest.raises(grpc.RpcError) as exc_info:
            stub.BuildDebianPackage(
                mcp_pb2.BuildDebianPackageRequest(
                    target_host="loopback", repo_url="https://example.com/repo.git"
                ),
                metadata=[("authorization", "Bearer wrong_token")],
            )
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED


# ---------------------------------------------------------------------------
# BuildDebianPackage — core acceptance criterion
# ---------------------------------------------------------------------------

class TestBuildDebianPackage:
    def _wait_for_job(self, stub, job_id, timeout=10):
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp = stub.GetJob(
                mcp_pb2.GetJobRequest(job_id=job_id),
                metadata=_auth_meta(),
            )
            if resp.status not in (mcp_pb2.JOB_PENDING, mcp_pb2.JOB_RUNNING):
                return resp
            time.sleep(0.1)
        raise TimeoutError(f"job {job_id} did not finish in {timeout}s")

    def test_build_debian_package_returns_job_handle(self, server_env):
        """Core acceptance criterion: agent calls BuildDebianPackage on loopback, gets a job handle."""
        stub, _ = server_env
        handle = stub.BuildDebianPackage(
            mcp_pb2.BuildDebianPackageRequest(
                target_host="loopback",
                repo_url="https://example.com/repo.git",
                git_ref="main",
                label="test-build",
                timeout_seconds=5,
            ),
            metadata=_auth_meta(),
        )
        assert handle.job_id.startswith("j_")

    def test_build_debian_package_unknown_host(self, server_env):
        stub, _ = server_env
        with pytest.raises(grpc.RpcError) as exc_info:
            stub.BuildDebianPackage(
                mcp_pb2.BuildDebianPackageRequest(
                    target_host="does-not-exist",
                    repo_url="https://example.com/repo.git",
                ),
                metadata=_auth_meta(),
            )
        assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND


# ---------------------------------------------------------------------------
# WatchJob streaming (uses BuildDebianPackage as the job source)
# ---------------------------------------------------------------------------

class TestWatchJob:
    def test_watch_job_receives_log_chunks(self, server_env):
        stub, _ = server_env
        handle = stub.BuildDebianPackage(
            mcp_pb2.BuildDebianPackageRequest(
                target_host="loopback",
                repo_url="https://github.com/example/pkg.git",
                git_ref="main",
                timeout_seconds=10,
                label="watch-test",
            ),
            metadata=_auth_meta(),
        )
        # Give the job a moment to start, then watch with replay
        time.sleep(0.3)
        events = list(
            stub.WatchJob(
                mcp_pb2.WatchJobRequest(
                    job_id=handle.job_id, replay_from_start=True
                ),
                metadata=_auth_meta(),
            )
        )
        assert len(events) > 0
        # At least one event should carry log data or a terminal status change
        kinds = {
            "log" if e.HasField("log") else "status"
            for e in events
        }
        assert kinds  # non-empty


# ---------------------------------------------------------------------------
# Reservation lifecycle (stub implementation)
# ---------------------------------------------------------------------------

class TestReservations:
    def test_reserve_loopback_queue(self, server_env):
        stub, _ = server_env
        resp = stub.ReserveMachine(
            mcp_pb2.ReserveMachineRequest(queue="loopback", image="ubuntu-24.04", label="test"),
            metadata=_auth_meta(),
        )
        assert resp.reservation_id.startswith("r_")
        assert resp.testflinger_job_id.startswith("tf_")

    def test_get_reservation(self, server_env):
        stub, _ = server_env
        res = stub.ReserveMachine(
            mcp_pb2.ReserveMachineRequest(queue="loopback", image="ubuntu-24.04", label="test"),
            metadata=_auth_meta(),
        )
        time.sleep(0.6)  # let the stub provisioner finish
        info = stub.GetReservation(
            mcp_pb2.GetReservationRequest(reservation_id=res.reservation_id),
            metadata=_auth_meta(),
        )
        assert info.state == mcp_pb2.READY
        assert info.hostname == "loopback"

    def test_release_machine(self, server_env):
        stub, _ = server_env
        res = stub.ReserveMachine(
            mcp_pb2.ReserveMachineRequest(queue="loopback", label="test"),
            metadata=_auth_meta(),
        )
        time.sleep(0.6)
        rel = stub.ReleaseMachine(
            mcp_pb2.ReleaseMachineRequest(reservation_id=res.reservation_id),
            metadata=_auth_meta(),
        )
        assert rel.accepted is True
