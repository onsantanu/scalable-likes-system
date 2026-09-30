from fastapi import HTTPException

from app.models.like import LikeEvent

ACTIONS = {"like", "unlike"}


def validate_ids(post_id: int, user_id: int) -> None:
    if post_id <= 0:
        raise HTTPException(status_code=422, detail="post_id must be positive")
    if user_id <= 0:
        raise HTTPException(status_code=422, detail="X-User-Id must be positive")


def parse_event(msg_id: str, fields: dict) -> LikeEvent:
    """Raises ValueError/KeyError/TypeError for malformed messages (treated as poison)."""
    action = fields["action"]
    if action not in ACTIONS:
        raise ValueError(f"unknown action {action!r}")
    return LikeEvent(
        msg_id=msg_id,
        post_id=int(fields["post_id"]),
        user_id=int(fields["user_id"]),
        action=action,
        ts_ms=int(fields["ts"]),
    )
