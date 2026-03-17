from pathlib import Path
import sys


def main() -> None:
    # Keep root entrypoint, but route to the active FIX engine.
    fix_engine_dir = Path(__file__).resolve().parent / "fix_engine"
    if str(fix_engine_dir) not in sys.path:
        sys.path.insert(0, str(fix_engine_dir))
    from main import run  # type: ignore

    run()


if __name__ == "__main__":
    main()

