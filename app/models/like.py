from dataclasses import dataclass

from pydantic import BaseModel


@dataclass
class LikeEvent:
    """One queue message, parsed."""
    msg_id: str
    post_id: int
    user_id: int
    action: str      # "like" | "unlike"
    ts_ms: int


class LikeActionResponse(BaseModel):
    post_id: int
    liked: bool
    queued: bool     # False when the flag already showed this state (nothing to do)


class LikeInfoResponse(BaseModel):
    post_id: int
    like_count: int
    liked_by_me: bool
