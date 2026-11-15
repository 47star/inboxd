from datetime import UTC
from email.utils import parsedate_to_datetime

import httpx

from inboxd.domain.work_items import WorkItem
from inboxd.sources.redmine.records import activity_signals, api_item, feed_items

DETAIL_INCLUDES = "children,attachments,relations,changesets,journals,watchers"


class IssueUnavailable(Exception):
    def __init__(self, identity: int, status: int):
        self.identity = identity
        self.status = status
        super().__init__(f"Redmine issue {identity} unavailable: HTTP {status}")


def filters() -> list[tuple[str, str]]:
    return [("set_filter", "1"), ("f[]", "status_id"), ("op[status_id]", "*")]


class RedmineReader:
    def __init__(self, client: httpx.AsyncClient, base_url: str, api_key: str, feed_key: str):
        self.client = client
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.feed_key = feed_key

    async def get(
        self,
        path: str,
        params: list[tuple[str, str]],
        api: bool = False,
        issue_id: int | None = None,
    ):
        headers = {"X-Redmine-API-Key": self.api_key} if api else {}
        params = params if api else [*params, ("key", self.feed_key)]

        try:
            response = await self.client.get(
                f"{self.base_url}/{path}", params=params, headers=headers
            )
        except httpx.HTTPError as error:
            raise RuntimeError(f"Redmine {path} transport failed: {type(error).__name__}") from None

        if issue_id is not None and response.status_code in {403, 404}:
            raise IssueUnavailable(issue_id, response.status_code)

        if response.status_code != 200:
            raise RuntimeError(f"Redmine {path} returned HTTP {response.status_code}")

        return response

    async def issue(self, identity: int) -> WorkItem:
        response = await self.get(
            f"issues/{identity}.json",
            [("include", DETAIL_INCLUDES)],
            api=True,
            issue_id=identity,
        )
        issue = response.json()["issue"]

        if int(issue["id"]) != identity:
            raise ValueError("Redmine returned a different issue ID")

        return api_item(issue, self.base_url)

    async def activity(self) -> list[tuple[int, str]]:
        response = await self.get(
            "activity.atom", [("show_issues", "1"), ("show_issue_notes", "1")]
        )

        return activity_signals(response.content, self.base_url)

    async def newest_id(self) -> int:
        response = await self.get(
            "issues.json", [*filters(), ("sort", "id:desc"), ("limit", "1")], api=True
        )
        issues = response.json()["issues"]

        return int(issues[0]["id"]) if issues else 0

    async def history(self, after_id: int, through_id: int) -> list[WorkItem]:
        params = [
            *filters(),
            ("sort", "id:asc"),
            ("limit", "100"),
            ("f[]", "issue_id"),
            ("op[issue_id]", "><"),
            ("v[issue_id][]", str(after_id + 1)),
            ("v[issue_id][]", str(through_id)),
        ]
        response = await self.get("issues.json", params, api=True)

        return [api_item(issue, self.base_url) for issue in response.json()["issues"]]

    async def feed(self, since: str, through_id: int | None = None) -> tuple[list[WorkItem], str]:
        params = [
            *filters(),
            ("sort", "id:desc"),
            ("f[]", "updated_on"),
            ("op[updated_on]", ">="),
            ("v[updated_on][]", since),
        ]

        if through_id is not None:
            params.extend(
                [
                    ("f[]", "issue_id"),
                    ("op[issue_id]", "<="),
                    ("v[issue_id][]", str(through_id)),
                ]
            )

        response = await self.get("issues.atom", params)
        server_time = parsedate_to_datetime(response.headers["date"])

        if server_time.tzinfo is None:
            raise ValueError("Redmine response Date requires a timezone")

        return feed_items(response.content, self.base_url), server_time.astimezone(UTC).isoformat()
