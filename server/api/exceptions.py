class AppException(Exception):
    """Base class for all custom exceptions."""

    def __init__(self, message: str, status_code: int) -> None:
        self.message = message
        self.status_code = status_code
        super().__init__(message)


class DatabaseException(AppException):
    """Exception raised for database related errors."""

    def __init__(
        self,
        message: str = "Database error",
        status_code: int = 500,
    ) -> None:
        super().__init__(message, status_code)


# class ForbiddenException(AppException):
#     """Exception raised for forbidden access."""

#     def __init__(
#         self, message: str, status_code: int = status.HTTP_403_FORBIDDEN
#     ) -> None:
#         super().__init__(message, status_code)


class BadRequestException(AppException):
    """Exception raised for bad requests."""

    def __init__(
        self, message: str, status_code: int = 400
    ) -> None:
        super().__init__(message, status_code)


# class UnauthorizedException(AppException):
#     """Exception raised for unauthorized access."""

#     def __init__(
#         self, message: str, status_code: int = status.HTTP_401_UNAUTHORIZED
#     ) -> None:
#         super().__init__(message, status_code)


class NotFoundException(AppException):
    """Exception raised for not found errors."""

    def __init__(
        self, message: str, status_code: int = 404
    ) -> None:
        super().__init__(message, status_code)


# class UnsupportedMediaTypeException(AppException):
#     """Exception raised for unsupported media type errors."""

#     def __init__(
#         self, message: str, status_code: int = status.HTTP_415_UNSUPPORTED_MEDIA_TYPE
#     ) -> None:
#         super().__init__(message, status_code)


# class ContentTooLargeException(AppException):
#     """Exception raised for content too large errors."""

#     def __init__(
#         self, message: str, status_code: int = status.HTTP_413_CONTENT_TOO_LARGE
#     ) -> None:
#         super().__init__(message, status_code)
