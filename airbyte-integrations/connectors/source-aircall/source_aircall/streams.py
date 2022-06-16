#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
import math
from abc import ABC, abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream


# Basic full refresh stream
class AircallStream(HttpStream, ABC):
    url_base = "https://api.aircall.io/v1/"
    per_page = 20

    # Aircall API has a limit in pagination
    # https://developer.aircall.io/api-references/#pagination
    AIRCALL_API_MAX_COUNT = 10000

    primary_key = 'id'

    def __init__(self, start_time: int, **kwargs):
        super().__init__(**kwargs)
        self.start_time = start_time
        self.current_page = 0

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        api_data = decoded_response[self.data_field]

        if len(api_data) < self.per_page:
            self.current_page = 0
            return None

        # At the end of Aircall allowed pagination, we need to increment 'from' parameter
        if self.current_page * self.per_page >= self.AIRCALL_API_MAX_COUNT:
            # We get last element creation date
            self.start_time = api_data[-1][self.creation_date_field]
            self.current_page = 0
            return {"page": self.current_page}
        else:
            self.current_page += 1

        return {"from": self.start_time, "page": self.current_page}

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        params = {"from": self.start_time, "per_page": self.per_page}

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

    @property
    def creation_date_field(self) -> str:
        """
        Defining a cursor field indicates that a stream is incremental, so any incremental stream must extend this class
        and define a cursor field.
        """
        return 'created_at'


class Calls(AircallStream):
    data_field = 'calls'
    creation_date_field = 'started_at'

    def path(self, **kwargs) -> str:
        return 'calls'

