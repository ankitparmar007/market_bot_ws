from typing import Any, Mapping, Optional
from pydantic import BaseModel, Field


class ResponseModel(BaseModel):
    status: bool
    message: str
    data: Optional[Mapping[str, Any]] = None


class SuccessResponse(ResponseModel):
    status: bool = Field(default=True, frozen=True)
    message: str = Field(default="Success")


class ErrorResponse(ResponseModel):
    status: bool = Field(default=False, frozen=True)
    message: str = Field(default="Error")
