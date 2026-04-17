"""Playwright backend 真源测试。"""

from __future__ import annotations

from types import SimpleNamespace
import sys
from typing import Any, cast

import pytest

from dayu.engine.tools import web_playwright_backend as backend_mod


class _FakeRoute:
    """测试用 Playwright route。"""

    def __init__(self, resource_type: str) -> None:
        """初始化 route。

        Args:
            resource_type: 资源类型。

        Returns:
            无。

        Raises:
            无。
        """

        self.request = SimpleNamespace(resource_type=resource_type)
        self.aborted = False
        self.continued = False

    def abort(self) -> None:
        """记录 abort 调用。"""

        self.aborted = True

    def continue_(self) -> None:
        """记录 continue 调用。"""

        self.continued = True


class _FakeProcess:
    """测试用子进程对象。"""

    def __init__(self, *, alive: bool, can_kill: bool = True) -> None:
        """初始化进程状态。

        Args:
            alive: 初始存活状态。
            can_kill: 是否暴露 kill 方法。

        Returns:
            无。

        Raises:
            无。
        """

        self._alive = alive
        self.join_calls: list[float] = []
        self.terminate_calls = 0
        self.kill_calls = 0
        if not can_kill:
            delattr(self, "kill")

    def is_alive(self) -> bool:
        """返回当前存活状态。"""

        return self._alive

    def join(self, timeout: float = 0) -> None:
        """记录 join 调用。"""

        self.join_calls.append(timeout)

    def terminate(self) -> None:
        """记录 terminate 调用。"""

        self.terminate_calls += 1
        self._alive = True

    def kill(self) -> None:
        """记录 kill 调用。"""

        self.kill_calls += 1
        self._alive = False


class _FakeClosable:
    """测试用可关闭对象。"""

    def __init__(self, *, method_name: str, should_raise: bool = False) -> None:
        """初始化对象。

        Args:
            method_name: 要暴露的方法名。
            should_raise: 是否在调用时抛异常。

        Returns:
            无。

        Raises:
            无。
        """

        self.calls = 0
        self.should_raise = should_raise
        setattr(self, method_name, self._call)

    def _call(self) -> None:
        """记录调用并按需抛异常。"""

        self.calls += 1
        if self.should_raise:
            raise RuntimeError("boom")


@pytest.mark.unit
def test_playwright_process_entry_puts_result_or_error() -> None:
    """验证子进程入口会回传结果或结构化错误。"""

    queue = SimpleNamespace(items=[])
    queue.put = queue.items.append

    backend_mod._playwright_process_entry(
        queue,
        lambda *, value: {"ok": True, "value": value},
        {"value": 1},
    )
    backend_mod._playwright_process_entry(
        queue,
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError(f"bad:{kwargs['value']}")),
        {"value": 2},
    )

    assert queue.items[0] == {"kind": "result", "payload": {"ok": True, "value": 1}}
    assert queue.items[1]["kind"] == "error"
    assert queue.items[1]["error_type"] == "RuntimeError"
    assert queue.items[1]["message"] == "bad:2"


@pytest.mark.unit
def test_playwright_process_helpers_cover_terminate_and_route_branching() -> None:
    """验证进程终止 helper 与资源路由 helper 的剩余分支。"""

    finished_process = _FakeProcess(alive=False)
    backend_mod._terminate_playwright_process(cast(Any, finished_process))
    assert finished_process.join_calls == [0]

    alive_process = _FakeProcess(alive=True)
    backend_mod._terminate_playwright_process(cast(Any, alive_process))
    assert alive_process.terminate_calls == 1
    assert alive_process.kill_calls == 1
    assert alive_process.join_calls == [backend_mod._PW_PROCESS_TERMINATE_GRACE_SECONDS, backend_mod._PW_PROCESS_TERMINATE_GRACE_SECONDS]

    image_route = _FakeRoute("image")
    document_route = _FakeRoute("document")
    backend_mod._route_handler_abort_resources(image_route)
    backend_mod._route_handler_abort_resources(document_route)
    assert image_route.aborted is True and image_route.continued is False
    assert document_route.aborted is False and document_route.continued is True
    assert backend_mod._normalize_playwright_channel(None) is None
    assert backend_mod._normalize_playwright_channel("  chrome  ") == "chrome"


@pytest.mark.unit
def test_close_playwright_browser_resets_globals_even_if_close_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """验证关闭浏览器 helper 会吞掉 close/stop 异常并重置单例。"""

    fake_browser = _FakeClosable(method_name="close", should_raise=True)
    fake_instance = _FakeClosable(method_name="stop", should_raise=True)
    monkeypatch.setattr(backend_mod, "_PW_BROWSER", fake_browser)
    monkeypatch.setattr(backend_mod, "_PW_INSTANCE", fake_instance)
    monkeypatch.setattr(backend_mod, "_PW_BROWSER_KEY", ("chrome", True))

    backend_mod._close_playwright_browser()

    assert fake_browser.calls == 1
    assert fake_instance.calls == 1
    assert backend_mod._PW_BROWSER is None
    assert backend_mod._PW_INSTANCE is None
    assert backend_mod._PW_BROWSER_KEY is None


@pytest.mark.unit
def test_get_playwright_browser_reuses_browser_and_handles_launch_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """验证浏览器单例会复用同 key 实例，并在初始化失败时返回 None。"""

    launches: list[dict[str, object]] = []
    fake_browser = SimpleNamespace(close=lambda: None)

    def _fake_launch(**kwargs: object) -> object:
        launches.append(dict(kwargs))
        return fake_browser

    fake_pw = SimpleNamespace(chromium=SimpleNamespace(launch=_fake_launch), stop=lambda: None)
    fake_sync_module = SimpleNamespace(sync_playwright=lambda: SimpleNamespace(start=lambda: fake_pw))
    monkeypatch.setitem(sys.modules, "playwright.sync_api", fake_sync_module)
    monkeypatch.setattr(backend_mod, "_PW_BROWSER", None)
    monkeypatch.setattr(backend_mod, "_PW_INSTANCE", None)
    monkeypatch.setattr(backend_mod, "_PW_BROWSER_KEY", None)

    browser = backend_mod._get_playwright_browser(playwright_channel=" chrome ", headless=False)
    reused_browser = backend_mod._get_playwright_browser(playwright_channel="chrome", headless=False)

    assert browser is fake_browser
    assert reused_browser is fake_browser
    assert launches == [
        {
            "headless": False,
            "channel": "chrome",
            "args": ["--disable-blink-features=AutomationControlled"],
        }
    ]

    warnings: list[str] = []
    failing_sync_module = SimpleNamespace(
        sync_playwright=lambda: SimpleNamespace(start=lambda: (_ for _ in ()).throw(RuntimeError("no browser")))
    )
    monkeypatch.setitem(sys.modules, "playwright.sync_api", failing_sync_module)
    monkeypatch.setattr(backend_mod, "_PW_BROWSER", None)
    monkeypatch.setattr(backend_mod, "_PW_INSTANCE", None)
    monkeypatch.setattr(backend_mod, "_PW_BROWSER_KEY", None)
    monkeypatch.setattr(backend_mod.Log, "warning", lambda message, *, module: warnings.append(f"{module}:{message}"))

    assert backend_mod._get_playwright_browser(playwright_channel=None, headless=True) is None
    assert any("Playwright 浏览器初始化失败" in message for message in warnings)