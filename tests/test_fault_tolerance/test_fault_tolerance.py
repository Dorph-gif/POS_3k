import os
import pytest
import asyncio
import json
import time
import requests
from httpx import AsyncClient
from fastapi.testclient import TestClient
from src.notification_service.main import app, send_with_http, send_with_kafka, LinkUpdated, link_updated
from unittest import mock
import responses
from fastapi import status
import httpx
from testcontainers.core.container import DockerContainer
from pathlib import Path

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TO_SERVER", "link_updates")
SERVER_URL = os.getenv("SERVER_URL", "http://localhost:8000")

TEST_PAYLOAD = {
    "link_id": 1,
    "url": "https://example.com",
    "last_update": "2024-01-01T00:00:00",
    "title": "Example",
    "user_name": "tester",
    "preview": "Sample preview"
}

@pytest.fixture(scope="module")
def test_client():
    with TestClient(app) as client:
        yield client

@responses.activate
def test_http_retry_logic(mocker):
    responses.add(responses.POST, f"{SERVER_URL}/api/v1/updated/", status=500)
    responses.add(responses.POST, f"{SERVER_URL}/api/v1/updated/", status=503)
    responses.add(responses.POST, f"{SERVER_URL}/api/v1/updated/", status=200)

    mock_get_links = mocker.patch(
        "src.notification_service.main.get_links_from_database"
    )
    mock_get_links.side_effect = [
        [123456789],
        []
    ]

    payload = LinkUpdated(**TEST_PAYLOAD)
    result = send_with_http(payload)

    assert result["status"] == "ok"
    assert len(responses.calls) == 3

@pytest.mark.asyncio
async def test_fallback_to_kafka_if_http_fails(mocker):
    mocker.patch(
        "src.notification_service.main.get_links_from_database",
        side_effect=[[111222333], []]
    )

    mocker.patch("src.notification_service.main.send_with_http", side_effect=Exception("HTTP failed"))

    mock_send_kafka = mocker.patch(
        "src.notification_service.main.send_with_kafka",
        return_value={"status": "fallback success"}
    )

    mocker.patch.dict("os.environ", {"MESSAGE_TRANSPORT": "HTTP"})

    payload = LinkUpdated(**TEST_PAYLOAD)
    response = await link_updated(payload)

    assert response == {"status": "fallback success"}
    assert mock_send_kafka.called

@responses.activate
def test_circuit_breaker_fast_failure(mocker):
    responses.add(
        responses.POST,
        "http://mock-server/api/v1/updated/",
        body=lambda req: time.sleep(5) or (200, {}, "OK"),
    )

    mocker.patch("src.notification_service.main.SERVER_URL", "http://mock-server")
    mocker.patch("src.notification_service.main.get_links_from_database", side_effect=[[123456789], []])

    mocker.patch.dict("os.environ", {
        "HTTP_TIMEOUT": "1",
        "RETRIES": "2",
        "BACKOFF_FACTOR": "0"
    })

    start = time.time()
    with pytest.raises(Exception):
        send_with_http(LinkUpdated(**TEST_PAYLOAD))
    duration = time.time() - start

    assert duration < 3.0

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

@pytest.mark.anyio
async def test_rate_limit_link_updated():
    image_name = "notification_service:test"

    import docker
    client = docker.from_env()
    client.images.build(path=str(PROJECT_ROOT / "notification_service"), tag=image_name)

    with DockerContainer(image_name)\
        .with_exposed_ports(8002)\
        .with_env("MESSAGE_TRANSPORT", "HTTP")\
        .with_env("SERVER_URL", "http://localhost:9999")\
        .with_env("BATCH_SIZE", "10")\
        .with_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092") as container:

        port = container.get_exposed_port(8002)
        base_url = f"http://localhost:{port}"

        for _ in range(30):
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.get(base_url + "/docs")
                    if resp.status_code == 200:
                        break
            except httpx.RequestError:
                time.sleep(1)
        else:
            raise RuntimeError("FastAPI не поднялся")

        payload = {
            "link_id": 1,
            "url": "http://example.com",
            "last_update": "2023-01-01T00:00:00Z",
            "title": "test",
            "user_name": "user",
            "preview": "preview"
        }

        async with httpx.AsyncClient(base_url=base_url) as client:
            for _ in range(5):
                r = await client.post("/api/v1/link_updated", json=payload)
                assert r.status_code in {200, 500}

            r = await client.post("/api/v1/link_updated", json=payload)
            assert r.status_code == 429
            assert r.json()["detail"] == "Too many requests, slow down!"