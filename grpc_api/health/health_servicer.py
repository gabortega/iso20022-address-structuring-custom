import logging

from grpc_health.v1 import health_pb2 as _health_pb2
from grpc_health.v1._async import HealthServicer

from grpc_api.server.address_structuring_servicer import AddressStructuringServicer

logger = logging.getLogger(__name__)

SERVICE_NAME = "address_structuring.AddressStructuring"


class AddressStructuringHealthServicer(HealthServicer):
    """
    Health check servicer that reports the health of the AddressStructuring service
    based on the status of its worker processes.
    """

    def __init__(self, address_structuring_servicer: AddressStructuringServicer) -> None:
        super().__init__()
        self._address_structuring_servicer = address_structuring_servicer

    async def Check(
            self, request: _health_pb2.HealthCheckRequest, context
    ) -> _health_pb2.HealthCheckResponse | None:
        """
        Override the default Check to dynamically evaluate worker health
        when the AddressStructuring service is queried.
        """
        if request.service in ("", SERVICE_NAME):
            servicer = self._address_structuring_servicer

            status = (_health_pb2.HealthCheckResponse.SERVING
                      if await servicer.monitor.handle_health_check()
                      else _health_pb2.HealthCheckResponse.NOT_SERVING)

            await self.set(SERVICE_NAME, status)
            await self.set("", status)

        return await super().Check(request, context)
