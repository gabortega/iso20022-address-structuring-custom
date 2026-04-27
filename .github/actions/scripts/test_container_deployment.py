import argparse
import time
import uuid

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

from grpc_api.generated import (pb2_AddressSample,
                                pb2_ProcessAddressResult,
                                pb2_grpc_AddressStructuringStub)


def _insecure_channel(ip_address: str, port: int | None = None) -> grpc.Channel:
    """Create an insecure channel to the given address with the RPC ID interceptor attached."""
    full_address = ip_address + (f":{port}" if port else "")
    channel = grpc.insecure_channel(full_address)
    return grpc.intercept_channel(channel)


def wait_for_server(ip_address: str, port: int | None = None, startup_timeout=60) -> None:
    """
    Poll the health endpoint until the server reports SERVING, then return the open channel.

    Raises RuntimeError if the server does not become healthy within startup_timeout seconds.
    """
    deadline = time.monotonic() + startup_timeout
    while time.monotonic() < deadline:
        try:
            with _insecure_channel(ip_address, port) as channel:
                health_stub = health_pb2_grpc.HealthStub(channel)
                request = health_pb2.HealthCheckRequest(service="address_structuring.AddressStructuring")
                resp: health_pb2.HealthCheckResponse = health_stub.Check(request)
                if resp.status == health_pb2.HealthCheckResponse.SERVING:
                    return
                elif resp.status == health_pb2.HealthCheckResponse.NOT_SERVING:
                    time.sleep(0.5)
                else:
                    raise RuntimeError(f"Unexpected status: {resp}")
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                time.sleep(0.5)
            else:
                raise e
    raise RuntimeError(f"Container deployment did not come online after waiting {startup_timeout}s")


def send_address(ip_address: str, address_sample: str, hash_id: str, port: int | None = None) -> list[
    pb2_ProcessAddressResult]:
    """Send a single address to the server via ProcessAddress and return all results."""
    with _insecure_channel(ip_address, port) as channel:
        stub = pb2_grpc_AddressStructuringStub(channel)
        sample = pb2_AddressSample(text=address_sample, hash_id=hash_id)
        metadata = [("client-rpc-id", str(uuid.uuid4()))]
        return list(stub.ProcessAddress(iter([sample]), metadata=metadata))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the address structuring container deployment.")
    parser.add_argument("--ip_address", "--fqdn", required=True, help="IP address/FQDN of the container deployment.")
    parser.add_argument("--port", required=False, help="Port that the container deployment is listening on.")
    args = parser.parse_args()

    address_sample_text = "SWIFT\nAVENUE ADELE 1\nLA HULPE, 1310\nBELGIQUE"
    address_hash_id = str(hash(address_sample_text))
    if args.port:
        port = int(args.port)
    else:
        port = None
    wait_for_server(ip_address=args.ip_address, port=port)
    results: list[pb2_ProcessAddressResult] = send_address(ip_address=args.ip_address,
                                                           port=port,
                                                           address_sample=address_sample_text,
                                                           hash_id=address_hash_id)
    assert len(results) == 1
    assert results[0].hash_id == address_hash_id
    assert len(results[0].matches) >= 1
    assert results[0].matches[0].country_match.resolved_name == "BE"
    assert results[0].matches[0].town_match.resolved_name == "LA HULPE"
