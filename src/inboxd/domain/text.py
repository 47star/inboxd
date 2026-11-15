from html.parser import HTMLParser

BLOCK_TAGS = {
    "p",
    "div",
    "li",
    "ul",
    "ol",
    "table",
    "tr",
    "td",
    "th",
    "pre",
    "blockquote",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "section",
    "article",
    "hr",
}


class PlainHTML(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []
        self.hidden = 0

    def handle_starttag(self, tag, attrs) -> None:
        if tag in {"script", "style"}:
            self.hidden += 1

        if not self.hidden and tag in BLOCK_TAGS | {"br"}:
            self.parts.append("\n")

    def handle_endtag(self, tag) -> None:
        if tag in {"script", "style"} and self.hidden:
            self.hidden -= 1

        if not self.hidden and tag in BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data) -> None:
        if not self.hidden:
            self.parts.append(data)


def plain_html(value: str) -> str:
    parser = PlainHTML()
    parser.feed(value)

    return "".join(parser.parts).strip()
