# Development Rules

- Keep Python code consistent with the Ruff rules and formatting in `pyproject.toml`.
- Keep Swift code consistent with `.swift-format`.
- Run `make format` after changes and `make check` before completion.
- In a new work directory, run `make setup` to configure the development environment and Git hooks.
- Commit Git changes by purpose. Do not bypass failed commit-hook checks.
- Do not avoid check failures with `noqa`, format exclusions, or disabled rules.
- Follow `scripts/check_layout.py` for blank lines around conditionals, loops, exception handling, and return statements.
- Keep related assignments together. Use blank lines when the role changes, such as preparation, processing, and result return.
- Split long expressions with intermediate variables and multiline arguments.
- Express intent with clear names and function separation without adding comments.
- Do not create source data, send messages, or restart operational daemons solely for syntax or formatting checks.
