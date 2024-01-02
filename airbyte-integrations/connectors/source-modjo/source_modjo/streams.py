#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from datetime import datetime, timedelta
from typing import Any, Iterable, Mapping, MutableMapping, Optional

import requests
from airbyte_cdk.sources.streams import IncrementalMixin
from airbyte_cdk.sources.streams.http import HttpStream

from source_modjo.authenticator import ModjoOauth2


# Incremental stream
class Calls(HttpStream, IncrementalMixin):
    url_base = "https://api.modjo.ai/"
    http_method = "POST"
    per_page = 100

    primary_key = 'id'
    cursor_field = 'date'

    def __init__(self, authenticator: ModjoOauth2):
        super().__init__(authenticator)
        self._authenticator = authenticator
        self._cursor_value = None
        self.start_date = None
        self.end_date = (datetime.utcnow() - timedelta(hours=2)).strftime('%Y-%m-%dT%H:%M:%S.000Z')

    def path(self, **kwargs):
        return 'calls/search'

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        pagination = decoded_response["pagination"]

        if pagination["page"] == pagination["lastPage"]:
            return None

        return {"page": pagination["nextPage"]}

    def request_body_json(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:

        params = {"pagination": {"perPage": self.per_page}, "filters": {}}

        if self.start_date:
            params["filters"]['startDate'] = self.start_date
        else:
            params["filters"]['startDate'] = "2000-01-01T00:00:00.000Z"
        params["filters"]['endDate'] = self.end_date

        # Handle pagination by inserting the next page's token in the request parameters
        if next_page_token:
            params["pagination"].update(next_page_token)
        else:
            params["pagination"].update({"page": 1})

        return params

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        records = response_json['values']

        for record in records:
            # Download transcript for each call
            record_id = record['id']
            details_url = f'https://api.modjo.ai/call-details/{record_id}'
            details_response = requests.get(details_url, headers=self._authenticator.get_auth_header())

            # raise and log exception if failure
            try:
                details_response.raise_for_status()
            except requests.HTTPError as exc:
                self.logger.error(response.text)
                raise exc

            details = details_response.json()

            # complete record with details
            for key in [
                # already existing in /calls/list but more detailed in /call-details
                'contacts',

                # new fields
                'createdOn',
                'modifiedOn',
                'externalPhoneProvider',
                'phoneProviderCallId',
                'phoneProviderRecordingUrl',
                'mediaUrl',
                'mediaFileS3Key',
                'status',
                'direction',
                'totalDuration',
                'wasTrimmed',
                'ownerId',
                'defaultContactId',
                'transcriptionJobName',
                'transcripts',
                'speakers',
            ]:
                record[key] = details[key]

            # If we want topicIds in transcripts to be filled, we have to call another endpoint
            # https://api.modjo.ai/transcription-blocks?callIds[]={record_id}
            # Not implemented yet

            # set state
            if self._cursor_value:
                self._cursor_value = max(self._cursor_value, record[self.cursor_field])
            else:
                self._cursor_value = record[self.cursor_field]

            yield record

    @property
    def state(self) -> MutableMapping[str, Any]:
        return {self.cursor_field: self._cursor_value}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        self._cursor_value = value[self.cursor_field]
        self.start_date = self._cursor_value


# Basic full refresh stream
class Tags(HttpStream):
    url_base = "https://api.modjo.ai/"
    per_page = 100
    primary_key = 'id'

    def __init__(self, authenticator: ModjoOauth2):
        super().__init__(authenticator)
        self.current_page = 1

    def path(self, **kwargs):
        return 'tags'

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()

        if len(decoded_response) < self.per_page:
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
        yield from response_json


# Basic full refresh stream
class Topics(HttpStream):
    url_base = "https://api.modjo.ai/"
    primary_key = 'id'

    def __init__(self, authenticator: ModjoOauth2):
        super().__init__(authenticator)

    def path(self, **kwargs):
        return 'topics'

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return {}

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json

