import argparse
import ast
from itertools import pairwise
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE_DIRECTORIES = ("src", "tests", "scripts")
BLOCKS = (
    ast.If,
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.With,
    ast.AsyncWith,
    ast.Try,
    ast.TryStar,
    ast.Match,
    ast.FunctionDef,
    ast.AsyncFunctionDef,
    ast.ClassDef,
)


def start_line(statement: ast.stmt) -> int:
    decorators = getattr(statement, "decorator_list", [])

    return decorators[0].lineno if decorators else statement.lineno


def missing_separators(source: str) -> list[int]:
    tree = ast.parse(source)
    lines = source.splitlines()
    missing = set()

    for node in ast.walk(tree):
        for _, value in ast.iter_fields(node):
            if not isinstance(value, list):
                continue

            statements = [item for item in value if isinstance(item, ast.stmt)]

            for previous, current in pairwise(statements):
                boundary = isinstance(previous, BLOCKS) or isinstance(
                    current, (*BLOCKS, ast.Return)
                )
                line = start_line(current)

                if not boundary or previous.end_lineno >= line:
                    continue

                gap = lines[previous.end_lineno : line - 1]

                if not any(not content.strip() for content in gap):
                    missing.add(line)

    return sorted(missing)


def separated(source: str, missing: list[int]) -> str:
    lines = source.splitlines(keepends=True)

    for line in reversed(missing):
        lines.insert(line - 1, "\n")

    return "".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Check blank lines at Python statement boundaries")
    parser.add_argument("--fix", action="store_true")
    arguments = parser.parse_args()
    failures = 0

    for directory in SOURCE_DIRECTORIES:
        for path in sorted((ROOT / directory).rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            missing = missing_separators(source)

            if arguments.fix and missing:
                path.write_text(separated(source, missing), encoding="utf-8")
                print(f"{path.relative_to(ROOT)}: separated {len(missing)} boundaries")
                continue

            for line in missing:
                print(f"{path.relative_to(ROOT)}:{line}: LAYOUT001 blank line required")
                failures += 1

    return int(failures > 0)


if __name__ == "__main__":
    raise SystemExit(main())
