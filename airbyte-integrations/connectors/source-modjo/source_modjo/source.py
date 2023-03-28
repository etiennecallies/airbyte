#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import Any, List, Mapping, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator, HttpAuthenticator

from source_modjo.streams import Calls


# Authentication
class ModjoAuthenticator:
    def get_auth(self, config: Mapping[str, Any]) -> HttpAuthenticator:
        email = config.get("email")
        if not email:
            raise Exception("No email")

        password = config.get("password")
        if not password:
            raise Exception("No password")

        response = requests.post(f"https://api.modjo.ai/auth/signin", json={
            'email': email,
            'password': password,
        })
        response_data = response.json()

        auth = Oauth2Authenticator('https://api.modjo.ai/auth/refresh', email, password, response_data['refresh_token'])

        return auth


# Source
class SourceModjo(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            authenticator = ModjoAuthenticator().get_auth(config)
            response = requests.get(f"https://api.modjo.ai/users/me", headers=authenticator.get_auth_header())
            response_data = response.json()

            return "firstName" in response_data, None
        except Exception as e:
            return False, repr(e)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = ModjoAuthenticator().get_auth(config)

        return [Calls(authenticator=authenticator)]
