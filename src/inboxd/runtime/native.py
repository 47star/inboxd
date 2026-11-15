import shutil
import subprocess
from importlib.resources import as_file, files
from pathlib import Path


def build_helper(bundle: Path | None = None) -> Path:
    packaged = files("inboxd").joinpath("native")

    if packaged.is_dir():
        with as_file(packaged) as source:
            return compile_helper(source, bundle)

    source = Path(__file__).resolve().parents[3] / "native"

    return compile_helper(source, bundle)


def compile_helper(source: Path, bundle: Path | None = None) -> Path:
    bundle = bundle or Path.home() / "Applications/inboxd-source.app"
    contents = bundle / "Contents"
    executable_dir = contents / "MacOS"
    resources = contents / "Resources"
    executable_dir.mkdir(parents=True, exist_ok=True)
    resources.mkdir(exist_ok=True)
    shutil.copy2(source / "Info.plist", contents / "Info.plist")
    shutil.copy2(source / "notes.js", resources / "notes.js")
    executable = executable_dir / "inboxd-source"
    subprocess.run(
        [
            "xcrun",
            "swiftc",
            "-O",
            "-target",
            "arm64-apple-macosx14.0",
            "-framework",
            "AppKit",
            "-framework",
            "EventKit",
            *[str(path) for path in sorted(source.glob("*.swift"))],
            "-o",
            str(executable),
        ],
        check=True,
    )
    subprocess.run(["codesign", "--force", "--sign", "-", str(bundle)], check=True)

    return executable
