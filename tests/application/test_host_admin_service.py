"""HostAdminService 测试。"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from dayu.contracts.events import AppEvent, AppEventType, PublishedRunEventProtocol
from dayu.contracts.run import RunCancelReason, RunRecord, RunState
from dayu.contracts.session import SessionRecord, SessionSource, SessionState
from dayu.host.event_bus import AsyncQueueEventBus
from dayu.host.host import Host
from dayu.host.protocols import LaneStatus
from dayu.services.contracts import HostCleanupResult
from dayu.services.host_admin_service import HostAdminService
from dayu.services.startup_recovery import recover_host_startup_state


class _FakeSessionRegistry:
    """测试用 session registry。"""

    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.records: dict[str, SessionRecord] = {
            "session_1": SessionRecord(
                session_id="session_1",
                source=SessionSource.WEB,
                state=SessionState.ACTIVE,
                scene_name="prompt",
                created_at=now,
                last_activity_at=now,
            )
        }

    def create_session(
        self,
        source: SessionSource,
        *,
        session_id: str | None = None,
        scene_name: str | None = None,
        metadata: dict[str, object] | None = None,
    ) -> SessionRecord:
        """创建会话。"""

        del metadata
        now = datetime.now(timezone.utc)
        record = SessionRecord(
            session_id=session_id or "session_created",
            source=source,
            state=SessionState.ACTIVE,
            scene_name=scene_name,
            created_at=now,
            last_activity_at=now,
        )
        self.records[record.session_id] = record
        return record

    def get_session(self, session_id: str) -> SessionRecord | None:
        """查询会话。"""

        return self.records.get(session_id)

    def list_sessions(self, *, state: SessionState | None = None) -> list[SessionRecord]:
        """列出会话。"""

        return [record for record in self.records.values() if state is None or record.state == state]

    def touch_session(self, session_id: str) -> None:
        """刷新活跃时间。"""

        record = self.records[session_id]
        self.records[session_id] = SessionRecord(
            session_id=record.session_id,
            source=record.source,
            state=record.state,
            scene_name=record.scene_name,
            created_at=record.created_at,
            last_activity_at=datetime.now(timezone.utc),
            metadata=record.metadata,
        )

    def close_session(self, session_id: str) -> None:
        """关闭会话。"""

        record = self.records[session_id]
        self.records[session_id] = SessionRecord(
            session_id=record.session_id,
            source=record.source,
            state=SessionState.CLOSED,
            scene_name=record.scene_name,
            created_at=record.created_at,
            last_activity_at=record.last_activity_at,
            metadata=record.metadata,
        )


class _FakeRunRegistry:
    """测试用 run registry。"""

    def __init__(self) -> None:
        now = datetime.now(timezone.utc)
        self.records: dict[str, RunRecord] = {
            "run_1": RunRecord(
                run_id="run_1",
                session_id="session_1",
                service_type="prompt",
                scene_name="prompt",
                state=RunState.RUNNING,
                created_at=now,
                started_at=now,
            ),
            "run_2": RunRecord(
                run_id="run_2",
                session_id="session_2",
                service_type="chat_turn",
                scene_name="interactive",
                state=RunState.SUCCEEDED,
                created_at=now,
                started_at=now,
                completed_at=now,
            ),
        }
        self.cleaned_orphans: list[str] = ["run_orphan_1"]

    def list_runs(
        self,
        *,
        session_id: str | None = None,
        state: RunState | None = None,
        service_type: str | None = None,
    ) -> list[RunRecord]:
        """列出运行。"""

        result = list(self.records.values())
        if session_id is not None:
            result = [record for record in result if record.session_id == session_id]
        if state is not None:
            result = [record for record in result if record.state == state]
        if service_type is not None:
            result = [record for record in result if record.service_type == service_type]
        return result

    def list_active_runs(self) -> list[RunRecord]:
        """列出活跃运行。"""

        return [record for record in self.records.values() if record.is_active()]

    def get_run(self, run_id: str) -> RunRecord | None:
        """查询单个运行。"""

        return self.records.get(run_id)

    def request_cancel(
        self,
        run_id: str,
        *,
        cancel_reason: RunCancelReason = RunCancelReason.USER_CANCELLED,
    ) -> bool:
        """请求取消运行。"""

        record = self.records.get(run_id)
        if record is None or record.is_terminal():
            return False
        self.records[run_id] = RunRecord(
            run_id=record.run_id,
            session_id=record.session_id,
            service_type=record.service_type,
            scene_name=record.scene_name,
            state=record.state,
            created_at=record.created_at,
            started_at=record.started_at,
            completed_at=record.completed_at,
            cancel_requested_at=datetime.now(timezone.utc),
            cancel_requested_reason=cancel_reason,
            cancel_reason=record.cancel_reason,
            metadata=record.metadata,
        )
        return True

    def cleanup_orphan_runs(self) -> list[str]:
        """清理孤儿运行。"""

        return list(self.cleaned_orphans)


@dataclass(frozen=True)
class _FakeGovernor:
    """测试用并发治理器。"""

    stale_permits: tuple[str, ...] = ("permit_1",)

    def cleanup_stale_permits(self) -> list[str]:
        """清理过期 permit。"""

        return list(self.stale_permits)

    def get_all_status(self) -> dict[str, LaneStatus]:
        """返回通道状态。"""

        return {
            "llm_api": LaneStatus(lane="llm_api", max_concurrent=4, active=1),
        }


def _build_service() -> tuple[HostAdminService, AsyncQueueEventBus]:
    """构建测试服务。"""

    session_registry = _FakeSessionRegistry()
    run_registry = _FakeRunRegistry()
    event_bus = AsyncQueueEventBus(run_registry=run_registry)  # type: ignore[arg-type]
    host = Host(
        executor=SimpleNamespace(),  # type: ignore[arg-type]
        session_registry=session_registry,  # type: ignore[arg-type]
        run_registry=run_registry,  # type: ignore[arg-type]
        concurrency_governor=_FakeGovernor(),  # type: ignore[arg-type]
        event_bus=event_bus,
    )
    return HostAdminService(host=host), event_bus


@pytest.mark.unit
def test_host_admin_service_lists_and_closes_sessions() -> None:
    """管理服务应能列出并关闭会话。"""

    service, _event_bus = _build_service()

    sessions = service.list_sessions(state="active")
    closed_session, cancelled_run_ids = service.close_session("session_1")

    assert [session.session_id for session in sessions] == ["session_1"]
    assert closed_session.state == "closed"
    assert cancelled_run_ids == ["run_1"]

    run = service.get_run("run_1")
    assert run is not None
    assert run.cancel_requested_at is not None
    assert run.cancel_requested_reason == "user_cancelled"
    assert run.cancel_reason is None


@pytest.mark.unit
def test_host_admin_service_lists_runs_and_builds_status() -> None:
    """管理服务应能列出运行并汇总宿主状态。"""

    service, _event_bus = _build_service()

    runs = service.list_runs(active_only=True)
    status = service.get_status()
    cleanup = service.cleanup()

    assert [run.run_id for run in runs] == ["run_1"]
    assert runs[0].cancel_reason is None
    assert status.active_session_count == 1
    assert status.active_run_count == 1
    assert status.active_runs_by_type == {"prompt": 1}
    assert status.lane_statuses["llm_api"].max_concurrent == 4
    assert cleanup.orphan_run_ids == ("run_orphan_1",)
    assert cleanup.stale_permit_ids == ("permit_1",)


@pytest.mark.unit
def test_recover_host_startup_state_logs_cleanup_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    """统一 startup recovery helper 应记录清理结果。"""

    service, _event_bus = _build_service()
    info_logs: list[str] = []
    monkeypatch.setattr("dayu.services.startup_recovery.Log.info", lambda message, *, module="APP": info_logs.append(message))

    result = recover_host_startup_state(
        service,
        runtime_label="CLI Host runtime",
        log_module="APP.MAIN",
    )

    assert result.orphan_run_ids == ("run_orphan_1",)
    assert result.stale_permit_ids == ("permit_1",)
    assert any("CLI Host runtime 启动恢复完成 orphan_runs=1 stale_permits=1" in message for message in info_logs)


@pytest.mark.unit
def test_recover_host_startup_state_warns_and_continues_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """统一 startup recovery helper 失败时应只告警并返回空结果。"""

    warnings: list[str] = []
    monkeypatch.setattr(
        "dayu.services.startup_recovery.Log.warning",
        lambda message, *, module="APP": warnings.append(message),
    )

    class _FailingHostAdminService:
        def cleanup(self) -> HostCleanupResult:
            raise RuntimeError("cleanup failed")

    result = recover_host_startup_state(
        _FailingHostAdminService(),  # type: ignore[arg-type]
        runtime_label="WeChat Host runtime",
        log_module="APP.WECHAT.MAIN",
    )

    assert result.orphan_run_ids == ()
    assert result.stale_permit_ids == ()
    assert any("WeChat Host runtime 启动恢复失败，将继续启动: cleanup failed" in message for message in warnings)


@pytest.mark.unit
def test_host_admin_service_rejects_invalid_session_source() -> None:
    """管理服务创建 session 时不应把非法来源静默降级成 web。"""

    service, _event_bus = _build_service()

    with pytest.raises(ValueError):
        service.create_session(source="bad-source")


@pytest.mark.unit
def test_host_admin_service_wraps_event_bus_subscription() -> None:
    """管理服务应把 Host event bus 包装成事件流。"""

    service, event_bus = _build_service()

    async def _collect() -> PublishedRunEventProtocol:
        stream = service.subscribe_run_events("run_1")
        publish_task = asyncio.create_task(_publish_later(event_bus))
        try:
            async for event in stream:
                return event
        finally:
            await publish_task
        raise AssertionError("未收到事件")

    event = asyncio.run(_collect())

    assert event.type == AppEventType.DONE
    assert event.payload == {"ok": True}


async def _publish_later(event_bus: AsyncQueueEventBus) -> None:
    """异步发布一条测试事件。"""

    await asyncio.sleep(0)
    event_bus.publish(
        "run_1",
        AppEvent(type=AppEventType.DONE, payload={"ok": True}, meta={}),
    )
