# Security policy

## Scope

Security reports may cover:

- credential exposure
- unauthorized access to the local HTTP API
- data exposure through MCP responses
- unsafe archive or attachment handling
- dependency vulnerabilities affecting inboxd

## Reporting

Do not open a public issue for a suspected vulnerability. Use the repository's private security reporting channel when it is configured. If no private channel is configured, contact the maintainers directly before sharing details.

Include:

- affected version or commit
- operating system and Python version
- reproduction steps
- impact
- logs or code excerpts with secrets and personal data removed

Do not send passwords, tokens, message contents, mail contents, or archive files.

## Local data

The archive can contain sensitive personal and work information. Protect the data directory and secrets file. Redact local data from bug reports and test fixtures.
