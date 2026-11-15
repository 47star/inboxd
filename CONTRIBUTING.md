# Contributing to inboxd

## Development environment

Requirements:

- Apple Silicon macOS 14 or later
- Python 3.12 or 3.13
- `uv`
- Xcode Command Line Tools

Set up the repository with:

```sh
make setup
```

## Workflow

1. Create a focused branch.
2. Make the smallest complete change.
3. Add or update meaningful tests.
4. Run `make format`.
5. Run `make check`.
6. Describe behavior changes, configuration changes, and verification in the pull request.

Do not include credentials, personal archives, source-service exports, generated data, or live logs in commits or issue reports.

## Code standards

- Follow the existing Python, Swift, and project layout conventions.
- Keep source configuration under `sources.<source_id>`.
- Use the collector plugin contract for new sources.
- Keep collection read-only with respect to source services.
- Preserve checkpoint and retry behavior when changing collectors.
- Do not bypass lint or formatting checks.

## Pull requests

Keep pull requests narrow and explain:

- the user-visible behavior
- the affected sources or APIs
- migration or configuration changes
- test commands and results
- known limitations

## Security reports

Do not publish credentials, private source data, or exploitable details in a public issue. Follow `SECURITY.md`.
