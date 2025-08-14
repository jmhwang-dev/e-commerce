from pydantic import BaseModel, Field

class Review(BaseModel):
    review_id: str
    order_id: str
    review_score: int = Field(ge=1, le=5)
    review_comment_title: str | None = None
    review_comment_message: str | None = None
    review_creation_date: str   # ISO "YYYY-MM-DD HH:MM:SS"
    review_answer_timestamp: str | None = None
