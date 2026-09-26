from fastapi.testclient import TestClient

from apps.cloud_demo.main import app


def test_dashboard_exposes_keyboard_and_screen_reader_semantics() -> None:
    page = TestClient(app).get("/")
    assert page.status_code == 200
    for required in (
        'lang="en"',
        'href="#main-content"',
        'id="main-content"',
        'aria-label="Primary navigation"',
        'role="status"',
        'aria-live="polite"',
        ':focus-visible',
        'prefers-reduced-motion:reduce',
        'forced-colors:active',
        'aria-hidden="true"',
    ):
        assert required in page.text
