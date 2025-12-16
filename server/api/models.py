class ResponseModel:
    def __init__(self, status: bool, message: str, data=None):
        self.status = status
        self.message = message
        self.data = data

    def to_dict(self):
        return {
            "status": self.status,
            "message": self.message,
            "data": self.data,
        }


class SuccessResponse(ResponseModel):
    def __init__(self, message="success", data=None):
        super().__init__(True, message, data)


class ErrorResponse(ResponseModel):
    def __init__(self, message="error"):
        super().__init__(False, message)
