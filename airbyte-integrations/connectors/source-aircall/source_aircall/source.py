#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import base64
from typing import Any, List, Mapping, Tuple

import requests
from requests.auth import AuthBase
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
        return [Calls(config.get('start_time', 0), authenticator=authenticator)]
