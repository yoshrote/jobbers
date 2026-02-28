"""Write the application's OpenAPI spec to a file without starting a real server."""
import asyncio
import json
import sys
from pathlib import Path

import httpx


def run() -> None:
    output_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("openapi.json")
    asyncio.run(_write_spec(output_path))


async def _write_spec(output_path: Path) -> None:
    from jobbers.task_routes import app

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
    ) as client:
        response = await client.get("/openapi.json")
        response.raise_for_status()

    output_path.write_text(json.dumps(response.json(), indent=2))
    print(f"OpenAPI spec written to {output_path}")


if __name__ == "__main__":
    run()
