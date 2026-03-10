# Use Python Slim image for smaller footprint
FROM python:3.12-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:0.8.13 /uv /uvx /bin/

# Copy the application into the container.
COPY . /app
COPY .env .env

# Install the application dependencies.
WORKDIR /app
RUN uv sync --frozen --no-cache

# Command to run your application
CMD ["uv", "run", "main.py"]
