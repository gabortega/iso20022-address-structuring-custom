import argparse
import time
import uuid

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

from grpc_api.generated import (pb2_AddressSample,
                                pb2_ProcessAddressResult,
                                pb2_grpc_AddressStructuringStub)


def _one_way_tls_channel(ip_address: str) -> grpc.Channel:
    """Create a one-way TLS channel to the given address with the RPC ID interceptor attached."""
    channel = grpc.secure_channel(f"{ip_address}", credentials=grpc.ssl_channel_credentials())
    return grpc.intercept_channel(channel)


def wait_for_server(ip_address: str, startup_timeout=60) -> None:
    """
    Poll the health endpoint until the server reports SERVING, then return the open channel.

    Raises RuntimeError if the server does not become healthy within startup_timeout seconds.
    """
    deadline = time.monotonic() + startup_timeout
    while time.monotonic() < deadline:
        try:
            with _one_way_tls_channel(ip_address) as channel:
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
    raise RuntimeError(f"Container App did not come online after waiting {startup_timeout}s")


def send_address(ip_address: str, address_sample: str, hash_id: str) -> list[pb2_ProcessAddressResult]:
    """Send a single address to the server via ProcessAddress and return all results."""
    with _one_way_tls_channel(ip_address) as channel:
        stub = pb2_grpc_AddressStructuringStub(channel)
        sample = pb2_AddressSample(text=address_sample, hash_id=hash_id)
        metadata = [("client-rpc-id", str(uuid.uuid4()))]
        return list(stub.ProcessAddress(iter([sample]), metadata=metadata))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the address structuring container app.")
    parser.add_argument("--ip_address", required=True, help="IP address of the container app.")
    args = parser.parse_args()

    address_sample_text = "SWIFT\nAVENUE ADELE 1\nLA HULPE, 1310\nBELGIQUE"
    address_hash_id = str(hash(address_sample_text))
    wait_for_server(ip_address=args.ip_address)
    results: list[pb2_ProcessAddressResult] = send_address(ip_address=args.ip_address,
                                                           address_sample=address_sample_text,
                                                           hash_id=address_hash_id)
    assert len(results) == 1
    assert results[0].hash_id == address_hash_id
    assert len(results[0].matches) >= 1
    assert results[0].matches[0].country_match.resolved_name == "BE"
    assert results[0].matches[0].town_match.resolved_name == "LA HULPE"
