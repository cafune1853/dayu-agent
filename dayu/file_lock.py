"""跨平台文本文件锁辅助。

统一封装 POSIX `fcntl.flock()` 与 Windows `msvcrt.locking()`，
供需要跨进程互斥的模块复用。
"""

from __future__ import annotations

import errno
import os
import time
from typing import Protocol, TextIO, cast

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows 不提供 fcntl
    fcntl = None

try:
    import msvcrt
except ImportError:  # pragma: no cover - POSIX 不提供 msvcrt
    msvcrt = None

_WINDOWS_LOCK_RETRY_INTERVAL_SEC = 0.1


class _MsvcrtLockingModule(Protocol):
    """Windows `msvcrt` 锁接口的最小协议。"""

    LK_NBLCK: int
    LK_UNLCK: int

    def locking(self, fd: int, mode: int, nbytes: int) -> None:
        """锁定或解锁给定字节区间。"""

        ...


def ensure_lock_region(stream: TextIO, *, region_bytes: int) -> None:
    """确保锁文件存在可锁定的固定字节区间。

    Args:
        stream: 已打开的锁文件流。
        region_bytes: 需要锁定的字节数。

    Returns:
        无。

    Raises:
        OSError: 当写入或同步失败时抛出。
    """

    if region_bytes <= 0:
        raise ValueError("region_bytes 必须大于 0")
    stream.seek(0, os.SEEK_END)
    if stream.tell() >= region_bytes:
        stream.seek(0)
        return
    stream.write("\0" * region_bytes)
    stream.flush()
    os.fsync(stream.fileno())
    stream.seek(0)


def is_lock_contention_error(exc: OSError) -> bool:
    """判断是否为跨进程文件锁竞争错误。

    Args:
        exc: 捕获到的底层 `OSError`。

    Returns:
        若错误由锁竞争触发则返回 `True`，否则返回 `False`。

    Raises:
        无。
    """

    return exc.errno in {errno.EACCES, errno.EAGAIN} or getattr(exc, "winerror", None) == 33


def acquire_text_file_lock(
    stream: TextIO,
    *,
    blocking: bool,
    region_bytes: int = 1,
    lock_name: str,
) -> None:
    """获取文本文件流上的跨平台排他锁。

    Args:
        stream: 已打开的文本流。
        blocking: 是否阻塞等待锁。
        region_bytes: Windows 下需要锁定的字节数。
        lock_name: 锁用途描述，用于错误提示。

    Returns:
        无。

    Raises:
        OSError: 当前平台没有可用锁实现，或底层加锁失败时抛出。
        ValueError: `region_bytes` 非法时抛出。
    """

    if fcntl is not None:
        lock_flags = fcntl.LOCK_EX
        if not blocking:
            lock_flags |= fcntl.LOCK_NB
        fcntl.flock(stream.fileno(), lock_flags)
        return
    if msvcrt is not None:
        msvcrt_module = cast(_MsvcrtLockingModule, msvcrt)
        ensure_lock_region(stream, region_bytes=region_bytes)
        while True:
            stream.seek(0)
            try:
                msvcrt_module.locking(stream.fileno(), msvcrt_module.LK_NBLCK, region_bytes)
                return
            except OSError as exc:
                if not blocking or not is_lock_contention_error(exc):
                    raise
                # Windows 的 LK_LOCK 最多重试 10 次，不符合 blocking=True 的语义；
                # 这里改为显式轮询 LK_NBLCK，直到真正拿到锁。
                time.sleep(_WINDOWS_LOCK_RETRY_INTERVAL_SEC)
        return
    raise OSError(f"当前平台不支持 {lock_name}")


def release_text_file_lock(
    stream: TextIO,
    *,
    region_bytes: int = 1,
    lock_name: str,
) -> None:
    """释放文本文件流上的跨平台排他锁。

    Args:
        stream: 已打开且已持锁的文本流。
        region_bytes: Windows 下需要解锁的字节数。
        lock_name: 锁用途描述，用于错误提示。

    Returns:
        无。

    Raises:
        OSError: 当前平台没有可用锁实现，或底层解锁失败时抛出。
        ValueError: `region_bytes` 非法时抛出。
    """

    if fcntl is not None:
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
        return
    if msvcrt is not None:
        msvcrt_module = cast(_MsvcrtLockingModule, msvcrt)
        stream.seek(0)
        msvcrt_module.locking(stream.fileno(), msvcrt_module.LK_UNLCK, region_bytes)
        return
    raise OSError(f"当前平台不支持 {lock_name}")
