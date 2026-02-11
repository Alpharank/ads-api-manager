"""
Lightweight Slack notification utility.

Reuses the pattern from auto-attribution-refactor but works standalone
(outside Airflow) by falling back to the SLACK_BOT_TOKEN env var.
"""

import logging
import os

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

logger = logging.getLogger(__name__)


class SlackNotifier:
    """Send Slack messages. Silent no-op when no token is available."""

    def __init__(self, token: str | None = None):
        if token is None:
            token = self._resolve_token()
        self.client = WebClient(token=token) if token else None

    @staticmethod
    def _resolve_token() -> str | None:
        """Try Airflow Variable first, then env var, then None."""
        # 1. Airflow Variable
        try:
            from airflow.models import Variable
            from airflow.exceptions import AirflowNotFoundException

            return Variable.get("SLACK_BOT_TOKEN")
        except Exception:
            pass

        # 2. Environment variable
        token = os.environ.get("SLACK_BOT_TOKEN")
        if token:
            return token

        logger.warning("SLACK_BOT_TOKEN not found — Slack notifications disabled")
        return None

    def send_message(self, message: str, channel: str) -> None:
        """Post a message to a Slack channel. Logs a warning and returns if
        no token is configured."""
        if self.client is None:
            logger.warning(
                "Slack client not initialised — skipping message to %s", channel
            )
            return

        try:
            response = self.client.chat_postMessage(channel=channel, text=message)
            if response["ok"]:
                logger.info("Slack message sent to %s", channel)
        except SlackApiError as e:
            logger.error("Error sending Slack message: %s", e.response["error"])
