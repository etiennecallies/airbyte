#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from abc import abstractmethod
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams import IncrementalMixin


# Basic full refresh stream
class AircallStream(HttpStream):
    url_base = "https://api.aircall.io/v1/"
    per_page = 20

    # Aircall API has a limit in pagination
    # https://developer.aircall.io/api-references/#pagination
    AIRCALL_API_MAX_COUNT = 10000

    primary_key = 'id'

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.start_time = config.get('start_time', 0)
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


class AircallIncrementalStream(AircallStream, IncrementalMixin):
    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(config, **kwargs)
        self._cursor_value = None

    @property
    @abstractmethod
    def data_field(self) -> str:
        pass

    @property
    def cursor_field(self) -> str:
        return self.creation_date_field

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_field: self._cursor_value}
        else:
            # In case of no results, we save the config start_time (0 by default)
            return {self.cursor_field: self.start_time}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]
        # We start fetching API from the most recent date of:
        # - the latest state date (the latest record creation date)
        # - the start date specified in config
        self.start_time = max(self.start_time, self._cursor_value)

    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if self._cursor_value:
                latest_record_date = record[self.cursor_field]
                self._cursor_value = max(self._cursor_value, latest_record_date)
            yield record


class Calls(AircallIncrementalStream):
    data_field = 'calls'
    creation_date_field = 'started_at'

    def path(self, **kwargs) -> str:
        return 'calls'

