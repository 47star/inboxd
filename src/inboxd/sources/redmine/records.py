import json
import re
import xml.etree.ElementTree as ET
from urllib.parse import urlsplit

from inboxd.domain.text import plain_html
from inboxd.domain.work_items import WorkItem
from inboxd.storage.timeline import timestamp

ATOM = {"a": "http://www.w3.org/2005/Atom"}


def api_item(issue: dict, base_url: str) -> WorkItem:
    updated = max(
        [timestamp(issue["updated_on"])]
        + [timestamp(journal["created_on"]) for journal in issue.get("journals", [])]
    )
    fields = canonical(issue)

    for collection in (
        "relations",
        "children",
        "journals",
        "attachments",
        "watchers",
        "changesets",
    ):
        fields.setdefault(collection, [])

    return WorkItem(
        external_id=str(issue["id"]),
        title=issue["subject"],
        description=issue.get("description") or "",
        description_format="redmine_markup",
        url=f"{base_url}/issues/{issue['id']}",
        updated_at=updated,
        created_at=timestamp(issue["created_on"]),
        project_name=issue.get("project", {}).get("name"),
        tracker_name=issue.get("tracker", {}).get("name"),
        status_name=issue.get("status", {}).get("name"),
        author_name=issue.get("author", {}).get("name"),
        fields=fields,
    )


def canonical(value):
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in value.items()}

    if isinstance(value, list):
        items = [canonical(item) for item in value]

        if items and all(isinstance(item, dict) and "id" in item for item in items):
            return sorted(items, key=lambda item: json.dumps(item["id"]))

        return items

    return value


def feed_items(content: bytes, base_url: str) -> list[WorkItem]:
    root = ET.fromstring(content)

    if root.tag != "{http://www.w3.org/2005/Atom}feed":
        raise ValueError("Redmine response is not an Atom issue feed")

    items = []
    base = urlsplit(base_url)
    issue_path = re.compile(re.escape(base.path.rstrip("/")) + r"/issues/(\d+)$")

    for entry in root.findall("a:entry", ATOM):
        link = next(
            (
                node.get("href", "")
                for node in entry.findall("a:link", ATOM)
                if node.get("rel", "alternate") == "alternate"
            ),
            entry.findtext("a:id", "", ATOM),
        )
        url = urlsplit(link)
        match = issue_path.fullmatch(url.path)

        if (url.scheme, url.netloc) != (base.scheme, base.netloc) or not match:
            raise ValueError("Redmine feed contains an unexpected issue link")

        identity = match[1]
        title = entry.findtext("a:title", "", ATOM)
        title_match = re.fullmatch(
            rf"(?P<prefix>.+) #{identity} \((?P<status>.*?)\): (?P<title>.*)",
            title,
            flags=re.DOTALL,
        )

        if not title_match or " - " not in title_match["prefix"]:
            raise ValueError(f"Redmine issue {identity} has an unsupported feed title")

        project, tracker = title_match["prefix"].rsplit(" - ", 1)
        body = entry.find("a:content", ATOM)

        if body is None or body.get("type", "text") not in {"html", "text"}:
            raise ValueError(f"Redmine issue {identity} has unsupported feed content")

        text = body.text or ""
        description = plain_html(text) if body.get("type") == "html" else text
        items.append(
            WorkItem(
                external_id=identity,
                title=title_match["title"],
                description=description,
                url=f"{base_url}/issues/{identity}",
                updated_at=timestamp(entry.findtext("a:updated", "", ATOM)),
                project_name=project,
                tracker_name=tracker,
                status_name=title_match["status"],
                author_name=entry.findtext("a:author/a:name", None, ATOM),
            )
        )

    return items


def activity_signals(content: bytes, base_url: str) -> list[tuple[int, str]]:
    from inboxd.domain.records import digest

    root = ET.fromstring(content)

    if root.tag != "{http://www.w3.org/2005/Atom}feed":
        raise ValueError("Redmine response is not an Atom activity feed")

    base = urlsplit(base_url)
    pattern = re.compile(re.escape(base.path.rstrip("/")) + r"/issues/(\d+)$")
    signals = []

    for entry in root.findall("a:entry", ATOM):
        for link in entry.findall("a:link", ATOM):
            url = urlsplit(link.get("href", ""))
            match = pattern.fullmatch(url.path)

            if match and (url.scheme, url.netloc) == (base.scheme, base.netloc):
                signals.append((int(match[1]), digest(ET.tostring(entry, encoding="unicode"))))
                break

    return signals
