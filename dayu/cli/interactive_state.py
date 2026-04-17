"""interactive UI 本地状态持久化。

该模块只负责 interactive UI 自己拥有的会话绑定状态：

- `interactive_key`：仅 interactive UI 自己理解的稳定键
- `scene_name`：固定为 `interactive`

模块不理解 Host Session 生命周期，也不负责业务决策。
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal


def build_interactive_key() -> str:
    """生成新的 interactive UI 绑定键。

    Args:
        无。

    Returns:
        新生成的稳定键文本。

    Raises:
        无。
    """

    return uuid.uuid4().hex


def build_interactive_session_id(interactive_key: str) -> str:
    """为 interactive 绑定键生成稳定的 Dayu session_id。

    Args:
        interactive_key: interactive UI 自己维护的绑定键。

    Returns:
        稳定的 Dayu session_id。

    Raises:
        ValueError: 当 `interactive_key` 为空时抛出。
    """

    normalized = str(interactive_key or "").strip()
    if not normalized:
        raise ValueError("interactive_key 不能为空")
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:16]
    return f"interactive_{digest}"


@dataclass(frozen=True)
class InteractiveSessionState:
    """interactive UI 当前绑定状态。"""

    interactive_key: str
    scene_name: Literal["interactive"] = "interactive"


class FileInteractiveStateStore:
    """基于文件系统的 interactive 状态仓储。"""

    def __init__(self, state_dir: Path) -> None:
        """初始化状态仓储。

        Args:
            state_dir: interactive 状态目录。

        Returns:
            无。

        Raises:
            无。
        """

        self._state_dir = Path(state_dir).expanduser().resolve()
        self._state_file = self._state_dir / "state.json"

    @property
    def state_dir(self) -> Path:
        """返回状态目录。"""

        return self._state_dir

    def load(self) -> InteractiveSessionState | None:
        """加载 interactive 状态。

        Args:
            无。

        Returns:
            状态对象；文件不存在时返回 `None`。

        Raises:
            ValueError: 当状态文件不是合法 JSON 对象，或字段缺失时抛出。
        """

        if not self._state_file.exists():
            return None
        raw = json.loads(self._state_file.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("interactive 状态文件必须是 JSON 对象")
        interactive_key = str(raw.get("interactive_key") or "").strip()
        scene_name = str(raw.get("scene_name") or "").strip() or "interactive"
        if not interactive_key:
            raise ValueError("interactive 状态缺少 interactive_key")
        if scene_name != "interactive":
            raise ValueError("interactive 状态的 scene_name 必须为 interactive")
        return InteractiveSessionState(
            interactive_key=interactive_key,
            scene_name="interactive",
        )

    def save(self, state: InteractiveSessionState) -> None:
        """保存 interactive 状态。

        Args:
            state: 待保存状态。

        Returns:
            无。

        Raises:
            无。
        """

        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._state_file.write_text(
            json.dumps(asdict(state), ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def clear(self) -> None:
        """删除 interactive 状态文件。

        Args:
            无。

        Returns:
            无。

        Raises:
            无。
        """

        if self._state_file.exists():
            self._state_file.unlink()


__all__ = [
    "FileInteractiveStateStore",
    "InteractiveSessionState",
    "build_interactive_key",
    "build_interactive_session_id",
]
