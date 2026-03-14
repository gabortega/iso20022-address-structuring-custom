import logging

from grpc_api.server.utils import set_logging_config, unset_logging_config


class TestSetLoggingConfig:

    def test_injects_rpc_id_into_log_format(self):
        """set_logging_config should include the RPC ID in the root logger's formatter."""
        set_logging_config("abc-123")

        root_logger = logging.getLogger()
        handler = root_logger.handlers[0]
        assert "[RPC-abc-123]" in handler.formatter._fmt

    def test_unset_restores_default_format(self):
        """unset_logging_config should remove the RPC ID from the log format."""
        set_logging_config("abc-123")
        unset_logging_config()

        root_logger = logging.getLogger()
        handler = root_logger.handlers[0]
        assert "RPC-" not in handler.formatter._fmt
