import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    sources = sorted(str(path) for path in (ROOT / "native").glob("*.swift"))
    subprocess.run(
        [
            "xcrun",
            "swiftc",
            "-typecheck",
            "-target",
            "arm64-apple-macosx14.0",
            "-framework",
            "AppKit",
            "-framework",
            "EventKit",
            *sources,
        ],
        check=True,
    )

    with tempfile.TemporaryDirectory(prefix="inboxd-syntax-") as directory:
        subprocess.run(
            [
                "osacompile",
                "-l",
                "JavaScript",
                "-o",
                str(Path(directory) / "notes.scpt"),
                str(ROOT / "native/notes.js"),
            ],
            check=True,
        )


if __name__ == "__main__":
    main()
