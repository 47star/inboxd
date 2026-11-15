import asyncio
from collections.abc import AsyncIterator

from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient
from slack_sdk.web.async_slack_response import AsyncSlackResponse


class SlackReader:
    def __init__(self, token: str) -> None:
        self.client = AsyncWebClient(token=token)
        self.rate_limits = 0

    async def call(self, method: str, **parameters) -> AsyncSlackResponse:
        while True:
            try:
                return await getattr(self.client, method)(**parameters)
            except SlackApiError as error:
                if error.response.status_code != 429:
                    raise

                self.rate_limits += 1
                delay = float(error.response.headers.get("Retry-After", 30))
                await asyncio.sleep(max(1, delay))

    async def pages(self, method: str, **parameters) -> AsyncIterator[AsyncSlackResponse]:
        cursor = ""

        while True:
            response = await self.call(method, cursor=cursor, **parameters)
            yield response

            cursor = response.get("response_metadata", {}).get("next_cursor", "")

            if not cursor:
                return
