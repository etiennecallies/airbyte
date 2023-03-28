#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from abc import abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class ModjoStream(HttpStream):
    url_base = "https://api.modjo.ai/"
    per_page = 20

    primary_key = 'id'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.current_page = 0

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        api_data = decoded_response[self.data_field]

        if len(api_data) < self.per_page:
            self.current_page = 0
            return None

        return {"page": self.current_page}

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        params = {"per_page": self.per_page}

        # Handle pagination by inserting the next page's token in the request parameters
        if next_page_token:
            params.update(next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json[self.data_field]

    @property
    @abstractmethod
    def data_field(self) -> str:
        """The response entry that contains useful data"""
        pass


class Calls(ModjoStream):
    data_field = 'values'

    def path(self, **kwargs) -> str:
        return 'calls/list'

