import contextlib

from oban.cli import main


def safe_main() -> None:
    with contextlib.suppress(KeyboardInterrupt):
        main()


if __name__ == "__main__":
    safe_main()
