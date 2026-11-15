# inboxd

> Warning: You use inboxd at your own risk. Users are responsible for evaluating the software, protecting their data and credentials, and handling any consequences of its operation.
>
> Development status: inboxd is under active development. Features, configuration, APIs, and behavior may change without notice.

inboxd is a local, read-only archive for personal communications and work records on macOS. It collects available source data, stores it locally, indexes it for search, and makes it searchable by AI agents through MCP.

## Highlights

- Read-only collection. inboxd does not send messages or create, edit, move, or delete records in source services.
- Durable local storage with RocksDB.
- Full-text and substring search with Tantivy.
- Incremental collection, checkpoints, reconciliation, retry handling, and local change history.
- Attachment preservation and text extraction.
- stdio MCP server backed by a local authenticated HTTP service.
- Collector plugins discovered through the `inboxd.collectors` entry-point group.
- macOS LaunchAgent and native EventKit, Messages, Mail, and Notes integration.

Source services remain authoritative. A complete local archive depends on source permissions, local macOS availability, network access, provider retention, and the collector's coverage rules.

## Supported platform

- Apple Silicon Mac
- macOS 14 or later
- Python 3.12 or 3.13
- `uv`
- Xcode Command Line Tools with `xcrun`, `swiftc`, and `codesign`

The native helper currently targets `arm64-apple-macosx14.0`. Linux, Windows, and Intel Mac installation are not supported.

## Install

From the repository root:

```sh
uv sync --locked --python 3.12
uv run inboxd build-helper
mkdir -p "$HOME/.config/inboxd"
```

Configure the sources you want to use, then install the per-user LaunchAgent:

```sh
uv run inboxd install
uv run inboxd status
```

`install` registers and starts the LaunchAgent. It stops and replaces an existing inboxd LaunchAgent. `build-helper` is separate and is not run automatically by `install`.

The default paths are:

```text
~/Applications/inboxd-source.app
~/Library/LaunchAgents/net.donghoon.inboxd.plist
~/Library/Application Support/inboxd/
```

The daemon and MCP server must use the same data directory and port.

## Source plugins

The core package provides the standard collectors. Additional packages can register collectors with the `inboxd.collectors` entry-point group.

Each source is an independent entry point. The core registry does not contain a source list or source-specific defaults. Each plugin provides:

- a source ID
- a collector factory
- a description shown by `list_sources`
- default source configuration
- optional content kinds and timestamp semantics

Installing or removing a plugin changes the available source set without changing core code. Set `sources.<source_id>.enabled = false` to disable one installed source while keeping its stored records searchable.

Plugin discovery is cached for the process lifetime. Restart the daemon after installing, removing, or repairing a plugin. Import failures, invalid definitions, duplicate source IDs, and collector initialization failures are reported per source; the remaining collectors and archive queries continue to run.

The archive persists source descriptions and content kinds. Removing a collector preserves queries and search-index rebuilding for its archived records. Set the source to `enabled = false` before uninstalling its package if its configuration remains in the file.

### Built-in sources

| Source | Data | Timestamp basis |
| --- | --- | --- |
| `messages` | Locally available iMessage and SMS records | Message sent time |
| `mail` | Local Apple Mail EMLX files | Email sent time |
| `slack` | Authorized Slack conversations and threads | Message sent time |
| `calendar` | EventKit calendar occurrences | Occurrence start |
| `notes` | Notes available through the Notes application | Note creation time |
| `reminders` | EventKit reminders | Reminder creation time |
| `google_calendar` | Google Calendar events and recurring instances | Occurrence start |
| `google_tasks` | Google Tasks, including completed and hidden tasks | Task update time |
| `redmine` | Accessible Redmine issues and journals | Latest issue or journal time |

Installed addons may add more rows to this list. Use `list_sources` to discover the active set.

## Configuration

Configuration is loaded from `~/.config/inboxd/config.toml`. Source-specific settings live under `sources.<source_id>`. A plugin owns its source defaults and may define additional keys.

Installed plugins validate configuration keys and value types before startup. Unknown keys, string booleans, non-finite intervals, and invalid ranges are rejected. Plugins can provide `config_constraints` for their defaults or a `config_schema` derived from `inboxd.sources.configuration.SourceOptions`. A disabled source whose package is absent retains its configuration for later reinstallation.

```toml
port = 48765
poll_seconds = 3
reconcile_seconds = 300
attachment_max_bytes = 104857600
diagnostic_retention_days = 30

[sources.messages]
enabled = true
poll_seconds = 3
reconcile_seconds = 300

[sources.slack]
enabled = true
reconcile_seconds = 3600
full_reconcile_seconds = 604800
event_poll_seconds = 0.2
connection_check_seconds = 10

[sources.google_calendar]
enabled = false
poll_seconds = 60
backfill_seconds = 1
start_year = 1970
future_years = 5

[sources.redmine]
enabled = false
url = "https://redmine.example.com/redmine"
poll_seconds = 60
reconcile_batch_size = 40
```

The complete built-in example is in [`config.example.toml`](config.example.toml). Do not overwrite an existing account configuration with the example file.

Set `enabled = false` under a source to stop its collector and attachment work. Existing records remain searchable.

General settings:

- `data_dir`: archive, index, attachment, token, and log directory.
- `port`: local HTTP port, default `48765`.
- `poll_seconds`: fallback collection interval.
- `reconcile_seconds`: fallback reconciliation interval.
- `attachment_max_bytes`: maximum preserved attachment size, default 100 MiB.
- `diagnostic_retention_days`: number of daily diagnostic log files to retain.

Path settings are `data_dir`, `messages_path`, `mail_path`, `apple_helper`, and `secrets_file`. Use absolute paths. `~` and `$HOME` inside TOML values are not expanded.

`INBOXD_DATA_DIR` overrides the configured `data_dir`.

Configuration is read when the daemon starts. Apply changes by reinstalling the LaunchAgent:

```sh
uv run inboxd install
```

## Secrets

Store credentials in `~/.config/inboxd/secrets.json` with file mode `0600`. Supported core keys include:

```json
{
  "slack_app_token": "xapp-...",
  "slack_user_token": "xoxp-...",
  "redmine_api_key": "...",
  "redmine_feed_key": "..."
}
```

Environment variables are also supported for core credentials:

- `INBOXD_SLACK_APP_TOKEN`
- `INBOXD_SLACK_USER_TOKEN`
- `INBOXD_REDMINE_API_KEY`
- `INBOXD_REDMINE_FEED_KEY`

Additional collector packages may define their own credential keys and environment variables.

## macOS permissions

The native helper needs:

- Full Calendar access
- Full Reminders access
- Notes automation access

The LaunchAgent process also needs access to the Messages database and Apple Mail directories under macOS Privacy and Security settings. Permission granted to the native helper does not grant Python access to those directories.

Messages and Mail are local-source collectors. Records or attachments that macOS has not downloaded are not available to inboxd.

## Google Calendar and Tasks

Enable the Calendar API and Tasks API in a Google Cloud project. Create a desktop OAuth client and run:

```sh
uv run inboxd google-auth --client-file /path/to/client_secret.json
```

Tokens are stored under `data_dir/google/` with restrictive permissions. Use `--no-browser` to print the authorization URL instead of opening a browser.

Google collectors perform an initial backfill, persist page checkpoints, and then use incremental synchronization where supported. Google records are read from the local cache during search and message retrieval.

## Slack

Slack requires both a Socket Mode app token and an authorized user token. A bot token cannot replace the user token when collecting user conversations.

Required secret keys:

```json
{
  "slack_app_token": "xapp-...",
  "slack_user_token": "xoxp-..."
}
```

The required Slack scopes depend on the conversations and metadata to be collected. Typical scopes include `channels:read`, `groups:read`, `im:read`, `mpim:read`, the corresponding history scopes, `users:read`, `users:read.email`, `team:read`, `files:read`, and `reactions:read`.

Slack collection combines Socket Mode events with incremental reconciliation and periodic full reconciliation. A connected socket does not mean historical backfill is complete. Check `backfilling`, `completed_channels`, `unavailable_channels`, and `last_success` in source status.

## Redmine

Configure the Redmine root URL under `sources.redmine.url`. Do not include `/issues`.

```toml
[sources.redmine]
enabled = true
url = "https://redmine.example.com/redmine"
poll_seconds = 60
reconcile_batch_size = 40
```

Redmine uses REST and Atom feeds. Initial collection stores checkpoints and issue metadata. Later collection uses feed signals followed by detail requests, with periodic reconciliation for changes that feeds do not expose.

## MCP

inboxd exposes a stdio MCP server. The daemon must already be running.

For OpenCode:

```sh
opencode mcp add inboxd --global -- "$PWD/.venv/bin/inboxd-mcp"
opencode mcp list
```

For manual configuration:

```json
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "servers": {
      "inboxd": {
        "type": "local",
        "command": ["/absolute/path/inboxd/.venv/bin/inboxd-mcp"],
        "cwd": "/absolute/path/inboxd"
      }
    }
  }
}
```

The MCP server uses a local bearer token stored in `data_dir/api-token`. It does not start the daemon. The local HTTP API listens on `127.0.0.1` and is not an HTTP MCP endpoint.

## MCP tools

| Tool | Purpose |
| --- | --- |
| `list_sources` | Discover installed sources, coverage, and collector state |
| `sync_status` | Read daemon, collection, and indexing status |
| `search_messages` | Search titles, bodies, comments, and extracted attachment text |
| `get_recent_records` | Browse recent records without a search term |
| `list_slack_conversations` | Resolve Slack channels and direct messages |
| `get_conversation` | Browse one conversation oldest first |
| `get_slack_thread` | Read a Slack thread |
| `get_message` | Read bounded body segments and optional metadata |
| `get_changes` | Read durable ingestion changes |
| `get_attachments` | Read attachment preservation and extraction status |
| `get_attachment_text` | Read extracted attachment text |

Recommended query flow:

1. Call `list_sources` for the relevant source and inspect coverage.
2. Call `search_messages` with `order="newest"` and a small limit when looking for the latest match.
3. Follow `read_request` or `next_request` for the complete body.
4. Use `include_metadata=true` only when source metadata or original markup is required.

Search terms must contain at least two characters. `match="all"` is the default. Use `match="any"` for alternate spellings or languages. `before` and `after` require timezone-aware ISO 8601 timestamps.

An empty result means no matching local record. Check source coverage, `pending_index`, backfill state, and permissions before treating it as absence from the original service.

## Data and privacy

The archive is stored locally under `data_dir`:

```text
rocksdb/             records, checkpoints, and change history
index/               Tantivy search indexes
attachments/         preserved attachment bytes
api-token            local HTTP authentication token
daemon.lock          single-daemon lock
logs/                diagnostics and process logs
```

Raw source payloads and metadata may contain sensitive information. Protect the data directory, secrets file, OAuth tokens, and generated logs. MCP responses are sent to the connected MCP client and may then be processed by an AI agent; local storage and downstream processing are separate concerns.

Stop the daemon before copying the data directory for backup. Restore the complete data directory and configuration while the daemon is stopped. Do not treat a live RocksDB directory copy as a consistent backup.

## Operations

```sh
uv run inboxd status
uv run inboxd monitor --seconds 600
launchctl print "gui/$(id -u)/net.donghoon.inboxd"
```

Logs:

```sh
tail -n 100 "$HOME/Library/Application Support/inboxd/logs/diagnostics.jsonl"
tail -n 100 "$HOME/Library/Application Support/inboxd/logs/stderr.log"
```

The daemon uses a per-data-directory lock. Stop the LaunchAgent before running `uv run inboxd serve` manually:

```sh
launchctl bootout "gui/$(id -u)/net.donghoon.inboxd"
uv run inboxd serve
```

Reinstall the LaunchAgent after manual operation:

```sh
uv run inboxd install
```

## Troubleshooting

### MCP connection fails

1. Run `uv run inboxd status`.
2. Check `launchctl print "gui/$(id -u)/net.donghoon.inboxd"`.
3. Confirm `.venv/bin/inboxd-mcp` exists.
4. Confirm the daemon and MCP process use the same data directory and port.
5. Check `diagnostics.jsonl`, `stderr.log`, and `api-token`.

HTTP 401 usually means that the daemon and MCP server use different token files. HTTP 403 for a browser origin is expected for the local stdio-only access path.

### Source access fails

Check the source status returned by `list_sources`. `configuration_required` indicates missing credentials or a missing native helper. `source_unavailable` indicates source access or permission failure. For Messages and Mail, verify macOS Full Disk Access for the actual LaunchAgent process.

### Search results are missing

Check `pending_index`, source backfill state, time filters, and search mode. Use `order="newest"` for latest records. Resolve Slack names with `list_slack_conversations` before searching by conversation.

## Development

```sh
make setup
make format
make check
```

`make check` runs Ruff, format checks, the layout checker, native checks, Swift formatting checks, and the Python test suite. The repository uses Python 3.12, Ruff 0.17.3, and the locked `uv` environment.

Build distribution artifacts:

```sh
uv build
```

## Known limitations

- Messages and Mail cover only data available locally on macOS.
- Slack checkpoints are conversation-oriented; resuming in the middle of every page or thread is not guaranteed.
- Slack offline changes and deletions are reconciled only within provider and collector limits.
- Attachment extraction supports selected formats and bounded input sizes.
- `ready` attachment state, successful collection, and completed search indexing are separate indicators.
- Power loss, sleep, network loss, and provider errors are retried, but not every long-running recovery scenario is exhaustively verified.
- Redmine stores fields and history exposed by the API and available to the configured account. Changes never retained by Redmine cannot be recovered.

## License

inboxd is licensed under the GNU General Public License, version 3 or any later version. See [`LICENSE`](LICENSE).

All trademarks referenced herein are the properties of their respective owners.
