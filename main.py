from pathlib import Path
import sys


def main() -> None:
    # Root entrypoint -> fix_engine/main.py::run
    fix_engine_dir = Path(__file__).resolve().parent / "fix_engine"
    if str(fix_engine_dir) not in sys.path:
        sys.path.insert(0, str(fix_engine_dir))
    from main import run  # type: ignore

    run()


if __name__ == "__main__":
    main()

