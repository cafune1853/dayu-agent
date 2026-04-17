"""upload_progress_helpers 模块单元测试。"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from dayu.fins.pipelines.upload_progress_helpers import (
    build_conversion_started_events,
    build_original_file_uploaded_events,
    should_emit_upload_file_event,
)
from dayu.fins.pipelines.docling_upload_service import UploadFileEventPayload


class TestBuildOriginalFileUploadedEvents:
    """测试 build_original_file_uploaded_events 函数。"""

    def test_build_events_normal_files(self, tmp_path: Path) -> None:
        """测试正常文件的事件构建。

        Args:
            tmp_path: pytest 临时目录 fixture。

        Returns:
            无。

        Raises:
            AssertionError: 断言失败时抛出。
        """

        # 创建测试文件
        file1 = tmp_path / "test1.pdf"
        file1.write_text("test content 1")
        file2 = tmp_path / "test2.txt"
        file2.write_text("test content 2")

        # 调用函数
        events = build_original_file_uploaded_events([file1, file2])

        # 验证结果
        assert len(events) == 2

        # 验证第一个事件
        assert events[0].event_type == "file_uploaded"
        assert events[0].name == "test1.pdf"
        assert events[0].payload["source"] == "original"
        assert "size" in events[0].payload
        assert events[0].payload["size"] == len("test content 1")
        assert events[0].payload["content_type"] == "application/pdf"

        # 验证第二个事件
        assert events[1].event_type == "file_uploaded"
        assert events[1].name == "test2.txt"
        assert events[1].payload["source"] == "original"
        assert "size" in events[1].payload
        assert events[1].payload["content_type"] == "text/plain"

    def test_build_events_stat_error(self, tmp_path: Path) -> None:
        """测试文件状态读取失败的处理（覆盖行34-36）。

        Args:
            tmp_path: pytest 临时目录 fixture。

        Returns:
            无。

        Raises:
            AssertionError: 断言失败时抛出。
        """

        # 创建测试文件
        test_file = tmp_path / "test.pdf"
        test_file.write_text("test content")

        # mock Path.stat() 抛出 OSError
        with patch.object(Path, "stat", side_effect=OSError("Permission denied")):
            events = build_original_file_uploaded_events([test_file])

        # 验证结果
        assert len(events) == 1
        assert events[0].event_type == "file_uploaded"
        assert events[0].name == "test.pdf"
        assert events[0].payload["source"] == "original"
        # 验证没有 size 字段（因为 stat 失败）
        assert "size" not in events[0].payload
        # 但仍有 content_type
        assert events[0].payload["content_type"] == "application/pdf"

    def test_build_events_no_content_type(self, tmp_path: Path) -> None:
        """测试无法判断 content_type 的文件（覆盖行64）。

        Args:
            tmp_path: pytest 临时目录 fixture。

        Returns:
            无。

        Raises:
            AssertionError: 断言失败时抛出。
        """

        # 创建无扩展名的文件
        test_file = tmp_path / "noextension"
        test_file.write_text("test content")

        # 调用函数
        events = build_original_file_uploaded_events([test_file])

        # 验证结果
        assert len(events) == 1
        assert events[0].event_type == "file_uploaded"
        assert events[0].name == "noextension"
        assert events[0].payload["source"] == "original"
        assert "size" in events[0].payload
        # 验证没有 content_type 字段（因为无法判断）
        assert "content_type" not in events[0].payload

    def test_build_events_empty_list(self) -> None:
        """测试空文件列表。

        Args:
            无。

        Returns:
            无。

        Raises:
            AssertionError: 断言失败时抛出。
        """

        events = build_original_file_uploaded_events([])
        assert events == []


class TestBuildConversionStartedEvents:
    """测试 build_conversion_started_events 函数。"""

    def test_build_conversion_events(self, tmp_path: Path) -> None:
        """测试 conversion_started 事件构建。"""

        file1 = tmp_path / "test1.pdf"
        file2 = tmp_path / "test2.txt"
        file1.write_text("test content 1")
        file2.write_text("test content 2")

        events = build_conversion_started_events([file1, file2])

        assert len(events) == 2
        assert events[0].event_type == "conversion_started"
        assert events[0].name == "test1.pdf"
        assert events[0].payload["source"] == "docling"
        assert events[0].payload["message"] == "正在 convert"
        assert events[1].event_type == "conversion_started"
        assert events[1].name == "test2.txt"


class TestShouldEmitUploadFileEvent:
    """测试 should_emit_upload_file_event 函数。"""

    def test_emit_non_file_uploaded_events(self) -> None:
        """测试非 file_uploaded 事件应该透传。

        Args:
            无。

        Returns:
            无。

        Raises:
            AssertionError: 断言失败时抛出。
        """

        event = UploadFileEventPayload(
            event_type="conversion_started",
            name="test.pdf",
            payload={},
        )
        assert should_emit_upload_file_event(event) is True

    def test_emit_converted_file_uploaded(self) -> None:
        """测试 source 为 converted 的事件应该透传。

        Args:
            无。

        Returns:
            无。

        Raises:
            AssertionError: 断言失败时抛出。
        """

        event = UploadFileEventPayload(
            event_type="file_uploaded",
            name="test.pdf",
            payload={"source": "converted"},
        )
        assert should_emit_upload_file_event(event) is True

    def test_filter_original_file_uploaded(self) -> None:
        """测试 source 为 original 的事件应该过滤。

        Args:
            无。

        Returns:
            无。

        Raises:
            AssertionError: 断言失败时抛出。
        """

        event = UploadFileEventPayload(
            event_type="file_uploaded",
            name="test.pdf",
            payload={"source": "original"},
        )
        assert should_emit_upload_file_event(event) is False

    def test_filter_original_case_insensitive(self) -> None:
        """测试 source 匹配不区分大小写。

        Args:
            无。

        Returns:
            无。

        Raises:
            AssertionError: 断言失败时抛出。
        """

        event = UploadFileEventPayload(
            event_type="file_uploaded",
            name="test.pdf",
            payload={"source": "ORIGINAL"},
        )
        assert should_emit_upload_file_event(event) is False

        event2 = UploadFileEventPayload(
            event_type="file_uploaded",
            name="test.pdf",
            payload={"source": "  Original  "},
        )
        assert should_emit_upload_file_event(event2) is False

    def test_emit_missing_source(self) -> None:
        """测试缺少 source 字段时应该透传。

        Args:
            无。

        Returns:
            无。

        Raises:
            AssertionError: 断言失败时抛出。
        """

        event = UploadFileEventPayload(
            event_type="file_uploaded",
            name="test.pdf",
            payload={},
        )
        assert should_emit_upload_file_event(event) is True
