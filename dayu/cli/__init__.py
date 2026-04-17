"""统一 CLI 入口包。"""

from . import main


def run_main() -> int:
    """运行统一 CLI 主入口。"""

    return main.main()


__all__ = [
    "main",
    "run_main",
]
