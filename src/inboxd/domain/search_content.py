import re

from inboxd.domain.text import plain_html

HTML_MARKUP = re.compile(
    r"</?(?:p|div|span|strong|em|br|ul|ol|li|table|tr|td|pre|code|h[1-6])(?:\s[^>]*|\s*/?)>",
    re.IGNORECASE,
)


def rendered_text(value: str, format_name: str) -> str:
    if format_name == "html" or (format_name == "redmine_markup" and HTML_MARKUP.search(value)):
        return plain_html(value).replace("\xa0", " ")

    return value


def search_content(record: dict, attachment_text: str = "") -> dict[str, str]:
    item = record.get("metadata", {}).get("work_item")
    body = record.get("text", "")
    comments = ""

    if item:
        format_name = item.get("description_format", "plain_text")
        body = rendered_text(item.get("description", ""), format_name)
        comments = "\n\n".join(
            rendered_text(journal["notes"], format_name)
            for journal in item.get("fields", {}).get("journals", [])
            if journal.get("notes")
        )

    for metadata in record.get("metadata", {}).values():
        if isinstance(metadata, dict) and metadata.get("comments"):
            comments = "\n\n".join(
                comment.get("text", "") for comment in metadata["comments"] if comment.get("text")
            )
            break

    return {
        "title": record.get("subject", ""),
        "body": body,
        "comments": comments,
        "attachment": attachment_text,
    }


def readable_body(record: dict) -> str:
    content = search_content(record)

    return "\n\n".join(value for value in (content["body"], content["comments"]) if value)
