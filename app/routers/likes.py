from fastapi import APIRouter, Header, HTTPException, Request

from app.dao.like_cache_dao import LikeCacheDao, PostNotFound
from app.models.like import LikeActionResponse, LikeInfoResponse
from app.validators.like_validator import validate_ids

router = APIRouter(prefix="/posts", tags=["likes"])


def _dao(request: Request) -> LikeCacheDao:
    return request.app.state.like_cache_dao


async def _set_like(request: Request, post_id: int, user_id: int, action: str):
    validate_ids(post_id, user_id)
    try:
        result = await _dao(request).set_like(post_id, user_id, action)
    except PostNotFound:
        raise HTTPException(status_code=404, detail="post not found")
    return LikeActionResponse(post_id=post_id, liked=(action == "like"), queued=(result == "OK"))


@router.post("/{post_id}/like", response_model=LikeActionResponse)
async def like(request: Request, post_id: int, x_user_id: int = Header(...)):
    return await _set_like(request, post_id, x_user_id, "like")


@router.delete("/{post_id}/like", response_model=LikeActionResponse)
async def unlike(request: Request, post_id: int, x_user_id: int = Header(...)):
    return await _set_like(request, post_id, x_user_id, "unlike")


@router.get("/{post_id}/likes", response_model=LikeInfoResponse)
async def like_info(request: Request, post_id: int, x_user_id: int = Header(...)):
    validate_ids(post_id, x_user_id)
    try:
        count, liked = await _dao(request).get_info(post_id, x_user_id)
    except PostNotFound:
        raise HTTPException(status_code=404, detail="post not found")
    return LikeInfoResponse(post_id=post_id, like_count=count, liked_by_me=liked)
