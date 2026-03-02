#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import base64
from typing import Any, List, Mapping, Tuple

import requests
from requests.auth import AuthBase
from airbyte_cdk.models import AirbyteStateMessage
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator

from .streams import Calls


# Authentication
class AircallAuthenticator:
    def get_auth(self, config: Mapping[str, Any]) -> AuthBase:
        api_id = config.get("api_id")
        if not api_id:
            raise Exception("No api_id")

        api_token = config.get("api_token")
        if not api_token:
            raise Exception("No api_token")
        auth_string = f"{api_id}:{api_token}".encode("utf8")
        b64_encoded = base64.b64encode(auth_string).decode("utf8")
        auth = TokenAuthenticator(token=b64_encoded, auth_method="Basic")

        return auth


# Source
class SourceAircall(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            authenticator = AircallAuthenticator().get_auth(config)
            response = requests.get(f"https://api.aircall.io/v1/ping", headers=authenticator.get_auth_header())
            response_data = response.json()
            return 'ping' in response_data and response_data, None
        except Exception as e:
            return False, repr(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = AircallAuthenticator().get_auth(config)
        return [Calls(config, authenticator=authenticator)]

    @classmethod
    def read_state(cls, state_path: str) -> List[AirbyteStateMessage]:
        """Override to handle legacy state format (dict instead of list)."""
        from airbyte_cdk.connector import BaseConnector
        from airbyte_cdk.models import AirbyteStateMessageSerializer

        if not state_path:
            return []

        state_obj = BaseConnector._read_json_file(state_path)
        if not state_obj:
            return []

        # Handle legacy global state format: {"stream_name": {"cursor_field": value}}
        # Convert to the new per-stream list format expected by CDK >= 7.x
        if isinstance(state_obj, dict):
            state_obj = [
                {
                    "type": "STREAM",
                    "stream": {
                        "stream_descriptor": {"name": stream_name},
                        "stream_state": stream_state if isinstance(stream_state, dict) else {},
                    }
                }
                for stream_name, stream_state in state_obj.items()
            ]

        parsed_state_messages = []
        for state in state_obj:
            parsed_message = AirbyteStateMessageSerializer.load(state)
            if not parsed_message.stream and not parsed_message.data and not parsed_message.global_:
                raise ValueError("AirbyteStateMessage should contain either a stream, global, or state field")
            parsed_state_messages.append(parsed_message)

        return parsed_state_messages
