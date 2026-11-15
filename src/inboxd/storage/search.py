from pathlib import Path

import tantivy

from inboxd.domain.query import MatchMode, SearchOrder, search_terms, time_value
from inboxd.domain.search_content import search_content
from inboxd.sources.registry import content_kinds

EXACT_FIELDS = (
    "id",
    "source",
    "account",
    "conversation_id",
    "sender",
    "timestamp",
)

WRITER_HEAP_BYTES = 16 * 1024 * 1024
SEARCH_SCHEMA_VERSION = 3
FIELD_BOOSTS = {"title": 3.0, "body": 1.0, "comments": 1.0, "attachment": 0.5}


def build_schema() -> tantivy.Schema:
    builder = tantivy.SchemaBuilder()

    for field in EXACT_FIELDS:
        builder.add_text_field(field, stored=True, tokenizer_name="raw")

    for field in FIELD_BOOSTS:
        builder.add_text_field(field, tokenizer_name="substring")

    builder.add_integer_field("source_time", stored=True, indexed=True, fast=True)

    return builder.build()


def substring_analyzer() -> tantivy.TextAnalyzer:
    return tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.whitespace()).build()


def positional_bigrams(text: str) -> str:
    text = text.lower()

    return " ".join(text[index : index + 2].encode("utf-8").hex() for index in range(len(text) - 1))


class SearchIndex:
    def __init__(self, path: Path, kinds: set[str] | None = None) -> None:
        self.content_kinds = content_kinds() if kinds is None else kinds
        path = path / f"v{SEARCH_SCHEMA_VERSION}"
        self.created = not (path / "meta.json").exists()
        path.mkdir(parents=True, exist_ok=True)

        self.schema = build_schema()
        self.index = tantivy.Index(self.schema, path=str(path))
        self.index.register_tokenizer("substring", substring_analyzer())

        self.writer = self.index.writer(
            heap_size=WRITER_HEAP_BYTES,
            num_threads=1,
        )

    def replace(self, record_id: str, record: dict | None, attachment_text: str) -> None:
        self.writer.delete_documents_by_term("id", record_id)

        if (
            record is None
            or record["deleted"]
            or record.get("kind", "message") not in self.content_kinds
        ):
            return

        fields = {field: [record[field]] for field in EXACT_FIELDS}
        fields.update(
            {
                name: [positional_bigrams(text)]
                for name, text in search_content(record, attachment_text).items()
            }
        )
        fields["source_time"] = [time_value(record["timestamp"])]

        self.writer.add_document(tantivy.Document(**fields))

    def commit(self) -> None:
        self.writer.commit()
        self.index.reload()

    def find(
        self,
        text: str,
        filters: dict[str, str | None],
        limit: int,
        offset: int,
        order: SearchOrder = "relevance",
        before: str | None = None,
        after: str | None = None,
        match: MatchMode = "all",
    ) -> tuple[int, list[tuple[str, float]]]:
        query = self.compile_query(text, filters, before, after, match)
        searcher = self.index.searcher()
        arguments = {"limit": limit, "offset": offset}

        if order != "relevance":
            arguments["order_by_field"] = "source_time"
            arguments["order"] = tantivy.Order.Desc if order == "newest" else tantivy.Order.Asc

        result = searcher.search(query, **arguments)

        hits = [(searcher.doc(address).to_dict()["id"][0], score) for score, address in result.hits]

        return result.count, hits

    def compile_query(
        self,
        text: str,
        filters: dict[str, str | None],
        before: str | None = None,
        after: str | None = None,
        match: MatchMode = "all",
    ) -> tantivy.Query:
        clauses = []

        terms = []

        for word in search_terms(text):
            escaped = positional_bigrams(word)
            clause = self.index.parse_query(
                f'"{escaped}"', list(FIELD_BOOSTS), field_boosts=FIELD_BOOSTS
            )
            terms.append((tantivy.Occur.Must if match == "all" else tantivy.Occur.Should, clause))

        if terms:
            clauses.append((tantivy.Occur.Must, tantivy.Query.boolean_query(terms)))

        lower = time_value(after) if after else None
        upper = time_value(before) if before else None

        if lower is not None and upper is not None and lower > upper:
            raise ValueError("after must not exceed before")

        if lower is not None or upper is not None:
            clauses.append(
                (
                    tantivy.Occur.Must,
                    tantivy.Query.range_query(
                        self.schema,
                        "source_time",
                        tantivy.FieldType.Integer,
                        lower_bound=lower,
                        upper_bound=upper,
                    ),
                )
            )

        for field, value in filters.items():
            if value is not None:
                clause = tantivy.Query.term_query(self.schema, field, value)
                clauses.append((tantivy.Occur.Must, clause))

        if not clauses:
            return tantivy.Query.all_query()

        return tantivy.Query.boolean_query(clauses)

    def close(self) -> None:
        self.writer.commit()
        self.writer.wait_merging_threads()
