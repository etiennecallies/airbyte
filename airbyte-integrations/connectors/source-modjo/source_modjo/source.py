#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import Any, List, Mapping, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from source_modjo.authenticator import ModjoAuthenticator
from source_modjo.streams import Calls


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
