#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#
from typing import Any, Mapping, Tuple

import pendulum
import requests
from requests.auth import AuthBase


# Authentication
class ModjoOauth2(AuthBase):
    """
    Generates OAuth2.0 access tokens from an OAuth2.0 refresh token and client credentials.
    The generated access token is attached to each request via the Authorization header.
    """
    MODJO_TOKEN_DURATION = 15 * 60

    def __init__(
        self,
        refresh_token: str
    ):
        self.refresh_token = refresh_token
        self._token_expiry_date = None
        self._access_token = None

    def __call__(self, request):
        request.headers.update(self.get_auth_header())
        return request

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"Authorization": f"Bearer {self.get_access_token()}"}

    def get_access_token(self):
        if self.token_has_expired():
            t0 = pendulum.now()
            access_token, refresh_token = self.refresh_access_token()
            self.refresh_token = refresh_token
            self._access_token = access_token
            self._token_expiry_date = t0.add(seconds=self.MODJO_TOKEN_DURATION)

        return self._access_token

    def token_has_expired(self) -> bool:
        return self._token_expiry_date is None or pendulum.now() > self._token_expiry_date

    def refresh_access_token(self) -> Tuple[str, str]:
        """
        returns a tuple of (access_token, token_lifespan_in_seconds)
        """
        token_refresh_endpoint = 'https://api.modjo.ai/auth/refresh'
        try:
            response = requests.request(method="POST", url=token_refresh_endpoint, data={"token": self.refresh_token})
            response.raise_for_status()
            response_json = response.json()
            return response_json['accessToken'], response_json['refreshToken']
        except Exception as e:
            raise Exception(f"Error while refreshing access token: {e}") from e


class ModjoAuthenticator:
    def get_auth(self, config: Mapping[str, Any]) -> ModjoOauth2:
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

        auth = ModjoOauth2(response_data['refreshToken'])

        return auth
