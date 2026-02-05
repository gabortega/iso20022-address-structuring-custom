"""
Entry point for the address structuring REST server.
"""
import uvicorn

from data_structuring.config import RunServerConfig


def _server():
    """
    Function called when the program is used as a server.
    Not meant to be used in any other way.
    """

    # Parse server application args
    server_args = RunServerConfig()

    uvicorn.run("rest.server.app:app",
                host=server_args.hostname,
                port=server_args.port)


if __name__ == "__main__":
    _server()
