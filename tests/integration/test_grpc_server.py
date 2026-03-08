import signal
import subprocess
import sys
import time
from pathlib import Path
from subprocess import Popen
from typing import Iterable, Any, Generator

import grpc
import pytest

from grpc_api.generated import (pb2_AddressSample,
                                pb2_ProcessAddressResult,
                                pb2_grpc_AddressStructuringStub)

SSL_DIR = Path(__file__).parent / "ssl"
PROJECT_ROOT = Path(__file__).parent.parent.parent

CA_CERT = SSL_DIR / "ca.pem"
SERVER_CERT = SSL_DIR / "server.pem"
SERVER_KEY = SSL_DIR / "server-key.pem"
CLIENT_CERT = SSL_DIR / "client.pem"
CLIENT_KEY = SSL_DIR / "client-key.pem"

# Ports for the different server instances (use high ports to avoid conflicts)
INSECURE_PORT = 50150
TLS_PORT = 50151
MTLS_PORT = 50152


def _start_server(port: int,
                  ssl_enabled: bool = False,
                  ca_cert_path: Path | None = None,
                  startup_timeout: float = 60) -> Popen:
    """Start run_server.py as a subprocess with the given config via env vars."""
    args = [
        sys.executable, "-m", "grpc_api.run_server",
        "--hostname", "127.0.0.1",
        "--port", str(port),
        "--ssl_enabled", str(ssl_enabled).lower(),
        "--pipeline_max_instances", "1",
    ]
    if ssl_enabled:
        args = args + [
            "--ssl_cert_path", str(SERVER_CERT),
            "--ssl_key_path", str(SERVER_KEY),
        ]
        if ca_cert_path:
            args = args + [
                "--ssl_ca_cert_path", str(ca_cert_path)
            ]
    process = Popen(
        args,
        cwd=str(PROJECT_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for the server to be ready by polling the port
    deadline = time.monotonic() + startup_timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                f"Server exited prematurely (rc={process.returncode}).\n"
                f"stdout: {stdout.decode()}\nstderr: {stderr.decode()}"
            )
        try:
            if ssl_enabled:
                if not ca_cert_path:
                    channel_function = _tls_channel
                else:
                    channel_function = _mtls_channel
            else:
                channel_function = _insecure_channel
            with channel_function() as channel:
                grpc.channel_ready_future(channel).result(timeout=1)
            return process
        except grpc.FutureTimeoutError:
            time.sleep(0.5)
        except Exception:
            time.sleep(0.5)

    process.kill()
    stdout, stderr = process.communicate()
    raise RuntimeError(
        f"Server did not become ready within {startup_timeout}s.\n"
        f"stdout: {stdout.decode()}\nstderr: {stderr.decode()}"
    )


def _stop_server(proc: Popen) -> None:
    """Gracefully stop the server subprocess."""
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def _insecure_channel() -> grpc.Channel:
    return grpc.insecure_channel(f"127.0.0.1:{INSECURE_PORT}")


def _tls_channel() -> grpc.Channel:
    channel_creds = grpc.ssl_channel_credentials(root_certificates=CA_CERT.read_bytes())
    return grpc.secure_channel(f"127.0.0.1:{TLS_PORT}", channel_creds)


def _mtls_channel() -> grpc.Channel:
    channel_creds = grpc.ssl_channel_credentials(
        root_certificates=CA_CERT.read_bytes(),
        private_key=CLIENT_KEY.read_bytes(),
        certificate_chain=CLIENT_CERT.read_bytes(),
    )
    return grpc.secure_channel(f"127.0.0.1:{MTLS_PORT}", channel_creds)


def _generate_samples(hash_id_prefix: str) -> Iterable[pb2_AddressSample]:
    return iter([pb2_AddressSample(text="SWIFT\nAVENUE ADELE 1\nLA HULPE, 1310\nBELGIQUE",
                                   hash_id=f"{hash_id_prefix}1"),
                 pb2_AddressSample(text="SWIFT\n6, THE CORN EXCHANGE, 55 MARK LN\nLONDON EC3R 7NE\nUNITED KINGDOM",
                                   hash_id=f"{hash_id_prefix}2")])


def _assert_results(results: list[pb2_ProcessAddressResult], hash_id_prefix: str) -> None:
    assert len(results) == 2

    assert results[0].hash_id == f"{hash_id_prefix}1"
    assert len(results[0].matches) >= 1
    assert results[0].matches[0].country_match.resolved_name == "BE"
    assert results[0].matches[0].town_match.resolved_name == "LA HULPE"

    assert results[1].hash_id == f"{hash_id_prefix}2"
    assert len(results[1].matches) >= 1
    assert results[1].matches[0].country_match.resolved_name == "GB"
    assert results[1].matches[0].town_match.resolved_name == "LONDON"


class TestGrpcServer:

    @pytest.fixture(scope="class")
    def insecure_server(self) -> Generator[Popen, Any, None]:
        proc = _start_server(port=INSECURE_PORT)
        yield proc
        _stop_server(proc)

    def test_connection_succeeds(self, insecure_server: Popen) -> None:
        """Client without any certificates can communicate."""
        with _insecure_channel() as channel:
            stub = pb2_grpc_AddressStructuringStub(channel)
            _assert_results(
                list(stub.ProcessAddress(_generate_samples("insecure"))),
                hash_id_prefix="insecure")


class TestGrpcServerTLS:

    @pytest.fixture(scope="class")
    def tls_server(self) -> Generator[Popen, Any, None]:
        proc = _start_server(port=TLS_PORT, ssl_enabled=True)
        yield proc
        _stop_server(proc)

    def test_tls_connection_succeeds(self, tls_server: Popen):
        """Client with the correct CA cert can communicate over TLS."""
        with _tls_channel() as channel:
            stub = pb2_grpc_AddressStructuringStub(channel)
            _assert_results(
                list(stub.ProcessAddress(_generate_samples("tls"))),
                hash_id_prefix="tls")

    def test_tls_rejects_insecure_client(self, tls_server: Popen):
        """An insecure client cannot complete an RPC against a TLS server."""
        with _insecure_channel() as channel:
            stub = pb2_grpc_AddressStructuringStub(channel)
            with pytest.raises(grpc.RpcError):
                list(stub.ProcessAddress(_generate_samples("tls")))


class TestGrpcServerMutualTLS:

    @pytest.fixture(scope="class")
    def mtls_server(self) -> Generator[Popen, Any, None]:
        proc = _start_server(port=MTLS_PORT, ssl_enabled=True, ca_cert_path=CA_CERT)
        yield proc
        _stop_server(proc)

    def test_mtls_with_valid_client_cert(self, mtls_server: Popen):
        """Client presenting a valid CA-signed certificate succeeds over mTLS."""
        with _mtls_channel() as channel:
            stub = pb2_grpc_AddressStructuringStub(channel)
            _assert_results(
                list(stub.ProcessAddress(_generate_samples("mtls"))),
                hash_id_prefix="mtls")

    def test_mtls_rejects_client_without_cert(self, mtls_server: Popen):
        """Client without a certificate is rejected by the mTLS server."""
        with _insecure_channel() as channel:
            stub = pb2_grpc_AddressStructuringStub(channel)
            with pytest.raises(grpc.RpcError):
                list(stub.ProcessAddress(_generate_samples("mtls")))
