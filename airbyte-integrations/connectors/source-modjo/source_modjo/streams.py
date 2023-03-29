#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams.http import HttpStream
from source_modjo.authenticator import ModjoOauth2


# Basic full refresh stream
class Calls(HttpStream):
    url_base = "https://api.modjo.ai/"
    per_page = 20

    primary_key = 'id'
    data_field = 'values'

    def path(self, **kwargs):
        return 'calls/list'

    def __init__(self, authenticator: ModjoOauth2):
        super().__init__(authenticator)
        self.current_page = 1
        self._authenticator = authenticator

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        api_data = decoded_response[self.data_field]

        if len(api_data) < self.per_page:
            self.current_page = 1
            return None

        self.current_page += 1

        return {"page": self.current_page}

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        params = {"perPage": self.per_page}

        # Handle pagination by inserting the next page's token in the request parameters
        if next_page_token:
            params.update(next_page_token)
        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        records = response_json[self.data_field]

        for record in records:
            # Download transcript for each call
            record_id = record['id']
            transcript_url = f'https://api.modjo.ai/transcription-segments/{record_id}?format=text%2Fsrt'
            sub_response = requests.get(transcript_url, headers=self._authenticator.get_auth_header())
            record['transcript'] = str(sub_response.text)

            yield record

