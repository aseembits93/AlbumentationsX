"""Telemetry client for tracking anonymous usage statistics."""

from __future__ import annotations

import contextlib
import time
from threading import Thread
from typing import Any

from albumentations.core.analytics.backends.mixpanel import MixpanelBackend
from albumentations.core.analytics.collectors import is_ci_environment, is_pytest_running
from albumentations.core.analytics.events import ComposeInitEvent
from albumentations.core.analytics.settings import settings
from albumentations.core.analytics.user_id import get_user_id_manager

"""Telemetry client for tracking anonymous usage statistics."""


class TelemetryClient:
    """Singleton client for collecting and sending telemetry data with rate limiting and deduplication.

    Using Mixpanel backend for better library telemetry support:
    - No parameter limits
    - No web stream complications
    - Full transform list tracking
    - Better suited for custom events
    """

    _instance = None
    _initialized = False

    def __new__(cls) -> TelemetryClient:
        """Create or return the singleton instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self) -> None:
        if not self._initialized:
            self.backend = MixpanelBackend()
            # Disable telemetry in CI/test environments
            self.enabled = not (is_ci_environment() or is_pytest_running())
            self.sent_pipelines: set[str] = set()  # Track sent pipeline hashes
            self.last_send_time: float = 0
            self.rate_limit: float = 30.0  # 30 seconds between sends
            self.user_id_manager = get_user_id_manager()
            self._initialized = True

    def track_compose_init(self, compose_data: dict[str, Any], telemetry: bool = True, use_thread: bool = True) -> None:
        """Track Compose initialization event with rate limiting and deduplication.

        Args:
            compose_data: Data collected from the Compose instance
            telemetry: Whether telemetry is enabled for this specific instance
            use_thread: If True, send telemetry in background thread (default)

        """
        # Short-circuit checks up front for performance
        if not self.enabled or not telemetry or not settings.telemetry_enabled:
            return

        user_id = self.user_id_manager.get_or_create_user_id()
        if user_id is None:
            return

        pipeline_hash = compose_data.get("pipeline_hash")
        # Early deduplication and rate limiting
        now = time.time()
        if (pipeline_hash and pipeline_hash in self.sent_pipelines) or (now - self.last_send_time < self.rate_limit):
            return

        # Add user ID and create event object
        compose_data["user_id"] = user_id
        event = ComposeInitEvent(**compose_data)

        # Actually send event
        if use_thread:
            Thread(target=self._send_event_thread, args=(event,), daemon=True).start()
        else:
            self._send_event(event)

        # Track pipeline and last send
        if pipeline_hash:
            self.sent_pipelines.add(pipeline_hash)
        self.last_send_time = now

    def _send_event_thread(self, event: ComposeInitEvent) -> None:
        """Send event in thread with proper error handling.

        Args:
            event: The event to send

        """
        with contextlib.suppress(Exception):
            # Silently ignore all errors in thread
            self._send_event(event)

    def _send_event(self, event: ComposeInitEvent) -> bool:
        """Send event to backend.

        Args:
            event: The event to send

        Returns:
            True if event was sent successfully, False otherwise

        """
        try:
            self.backend.send_event(event)
            return True
        except (OSError, ValueError):
            return False

    def disable(self) -> None:
        """Disable telemetry collection."""
        self.enabled = False

    def enable(self) -> None:
        """Enable telemetry collection."""
        self.enabled = True

    def reset(self) -> None:
        """Reset the telemetry client state (mainly for testing)."""
        self.sent_pipelines.clear()
        self.last_send_time = 0


# Global telemetry client instance
telemetry_client = None


def get_telemetry_client() -> TelemetryClient:
    """Get or create the global telemetry client.

    Returns:
        The global TelemetryClient instance

    """
    global telemetry_client  # noqa: PLW0603
    if telemetry_client is None:
        telemetry_client = TelemetryClient()
    return telemetry_client


_CACHED_DISABLE_TELEMETRY = is_ci_environment() or is_pytest_running()
