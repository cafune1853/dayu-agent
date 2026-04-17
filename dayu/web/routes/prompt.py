"""Prompt 操作端点。"""

from __future__ import annotations

import asyncio

from dayu.services.protocols import PromptServiceProtocol


def create_prompt_router(prompt_service: PromptServiceProtocol):
    """创建 prompt 路由。

    Args:
        无。

    Returns:
        FastAPI 路由对象。

    Raises:
        无。
    """

    from fastapi import APIRouter, HTTPException
    from pydantic import BaseModel

    from dayu.services.contracts import PromptRequest

    router = APIRouter(prefix="/api", tags=["prompt"])

    class PromptRequestBody(BaseModel):
        """Prompt 请求体。"""

        user_text: str
        ticker: str | None = None

    class PromptResponse(BaseModel):
        """Prompt 响应（异步模式）。"""

        session_id: str
        accepted: bool = True

    @router.post("/prompt", response_model=PromptResponse, status_code=202)
    async def submit_prompt(body: PromptRequestBody) -> PromptResponse:
        """提交 prompt，结果通过 SSE 推送。

        Args:
            body: 请求体。

        Returns:
            可订阅的 session 句柄。

        Raises:
            无。
        """

        try:
            submission = await prompt_service.submit(
                PromptRequest(
                    user_text=body.user_text,
                    ticker=body.ticker,
                )
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        asyncio.create_task(_consume_stream(submission.event_stream))
        return PromptResponse(session_id=submission.session_id)

    return router


async def _consume_stream(stream):
    """后台消费流式事件。

    Args:
        stream: 事件流句柄。

    Returns:
        无。

    Raises:
        无。
    """

    async for _ in stream:
        pass


__all__ = ["create_prompt_router"]
