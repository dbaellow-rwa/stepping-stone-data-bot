# config/app_config.py
from __future__ import annotations

import os
from dataclasses import dataclass

"""

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.chatbot_question_log` (
  event_timestamp     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS (description="Server time when the question was processed"),
  question_id         STRING OPTIONS (description="UUID for this question/turn if available"),
  user_id             STRING OPTIONS (description="Authenticated user id or email (if available)"),
  session_id          STRING OPTIONS (description="Client/session identifier (cookie or oauth session id)"),
  is_follow_up        BOOL   OPTIONS (description="True if this turn is a follow-up"),
  previous_question   STRING OPTIONS (description="Free-text of previous question if follow-up"),
  question_text       STRING OPTIONS (description="Original user prompt/question"),
  generated_sql       STRING OPTIONS (description="SQL produced for this question"),
  summary_md          STRING OPTIONS (description="Markdown summary returned to the user"),
  context_history     STRING OPTIONS (description="Flattened conversational context passed to LLM"),
  rows_returned       INT64  OPTIONS (description="Row count of the final DataFrame shown to the user"),
  attempt_count       INT64  OPTIONS (description="How many attempts/retries this answer required"),
  latency_seconds     INT64  OPTIONS (description="Wall-clock seconds from submit to answer"),
  app_version         STRING OPTIONS (description="App/build version tag if set"),
  user_agent          STRING OPTIONS (description="Caller user agent if captured"),
  ip_hash             STRING OPTIONS (description="Hashed IP for coarse telemetry (optional)"),
  extra_metadata      JSON   OPTIONS (description="Any additional key/values")
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY is_follow_up, user_id, session_id;

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.chatbot_error_log` (
  event_timestamp     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS (description="Server time when the error was logged"),
  question_id         STRING OPTIONS (description="UUID of the question turn that failed"),
  user_id             STRING OPTIONS (description="Authenticated user id/email (if available)"),
  session_id          STRING OPTIONS (description="Client/session identifier"),
  question_text       STRING OPTIONS (description="User question at time of error"),
  generated_sql       STRING OPTIONS (description="SQL that triggered the error (if any)"),
  error_message       STRING OPTIONS (description="Top-level error text/exception message"),
  error_type          STRING OPTIONS (description="Short classifier, e.g., BQ_QUERY_ERROR, SAFETY_BLOCKED"),
  attempt_number      INT64  OPTIONS (description="Attempt index when this error occurred"),
  stack_trace         STRING OPTIONS (description="Optional traceback text"),
  app_version         STRING OPTIONS (description="App/build version tag if set"),
  extra_metadata      JSON   OPTIONS (description="Any additional key/values")
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY error_type, user_id, session_id;

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.chatbot_zero_result_log` (
  event_timestamp     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS (description="Server time when zero-result was observed"),
  question_id         STRING OPTIONS (description="UUID of the question turn that returned zero rows"),
  user_id             STRING OPTIONS (description="Authenticated user id/email (if available)"),
  session_id          STRING OPTIONS (description="Client/session identifier"),
  question_text       STRING OPTIONS (description="User question at time of zero-result"),
  generated_sql       STRING OPTIONS (description="SQL that returned zero rows"),
  attempt_number      INT64  OPTIONS (description="Attempt index when zero-result occurred"),
  app_version         STRING OPTIONS (description="App/build version tag if set"),
  extra_metadata      JSON   OPTIONS (description="Any additional key/values")
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY user_id, session_id;

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.chatbot_vote_feedback` (
  event_timestamp     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP() OPTIONS (description="Server time when the vote was recorded"),
  question_id         STRING OPTIONS (description="UUID of the question turn being rated"),
  user_id             STRING OPTIONS (description="Authenticated user id/email (if available)"),
  session_id          STRING OPTIONS (description="Client/session identifier"),
  vote                STRING OPTIONS (description="UP or DOWN (or a 1–5 scale if you expand later)"),
  question_text       STRING OPTIONS (description="Question that was rated"),
  summary_md          STRING OPTIONS (description="Answer/summary text that was rated"),
  reason_free_text    STRING OPTIONS (description="Optional user-supplied reason/comment"),
  app_version         STRING OPTIONS (description="App/build version tag if set"),
  extra_metadata      JSON   OPTIONS (description="Any additional key/values")
)
PARTITION BY DATE(event_timestamp)
CLUSTER BY vote, user_id, session_id;

"""

# ──────────────────────────────────────────────────────────────────────────────
# Helper(s)
# ──────────────────────────────────────────────────────────────────────────────
def _env(name: str, default: str | None = None) -> str | None:
    val = os.environ.get(name)
    return val if (val is not None and val != "") else default

def fqn(project: str | None, dataset: str, table: str) -> str:
    """Build fully-qualified table name. If project is None, return dataset.table."""
    return f"{project}.{dataset}.{table}" if project else f"{dataset}.{table}"

# ──────────────────────────────────────────────────────────────────────────────
# App Config (generic)
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class AppConfig:
    """
    Generic application settings for any client.

    Configure primarily via environment variables:
      APP_ENV                = "local" | "prod" (default: "prod")
      GOOGLE_CLOUD_PROJECT   = "<gcp-project-id>" (optional if using ADC default)
      BQ_LOG_DATASET         = "<dataset for logs>" (default: "app_logs")
      BQ_LOG_TABLE_ERROR     = "<table name>" (default: "chatbot_error_log")
      BQ_LOG_TABLE_ZERO      = "<table name>" (default: "chatbot_zero_result_log")
      BQ_LOG_TABLE_QUESTION  = "<table name>" (default: "chatbot_question_log")
      BQ_LOG_TABLE_VOTE      = "<table name>" (default: "chatbot_vote_feedback")

    Example:
      GOOGLE_CLOUD_PROJECT=my-retail-project
      BQ_LOG_DATASET=retail_ops
      BQ_LOG_TABLE_ERROR=bot_error_log
    """
    # Environment: "local" for dev, "prod" for deployed
    app_env: str = _env("APP_ENV", "prod")

    # BigQuery project (optional if ADC default project is set)
    gcp_project: str | None = _env("GOOGLE_CLOUD_PROJECT")

    # Dataset used for logging tables
    bq_log_dataset: str = _env("BQ_LOG_DATASET", "app_logs")

    # Log table *names* (not fully qualified)
    bq_log_table_error: str = _env("BQ_LOG_TABLE_ERROR", "chatbot_error_log")
    bq_log_table_zero: str = _env("BQ_LOG_TABLE_ZERO", "chatbot_zero_result_log")
    bq_log_table_question: str = _env("BQ_LOG_TABLE_QUESTION", "chatbot_question_log")
    bq_log_table_vote: str = _env("BQ_LOG_TABLE_VOTE", "chatbot_vote_feedback")

    # Computed properties for fully-qualified names
    @property
    def BQ_CHATBOT_ERROR_LOG(self) -> str:
        return fqn(self.gcp_project, self.bq_log_dataset, self.bq_log_table_error)

    @property
    def BQ_CHATBOT_ZERO_RESULT_LOG(self) -> str:
        return fqn(self.gcp_project, self.bq_log_dataset, self.bq_log_table_zero)

    @property
    def BQ_CHATBOT_QUESTION_LOG(self) -> str:
        return fqn(self.gcp_project, self.bq_log_dataset, self.bq_log_table_question)

    @property
    def BQ_CHATBOT_VOTE_FEEDBACK(self) -> str:
        return fqn(self.gcp_project, self.bq_log_dataset, self.bq_log_table_vote)

    @property
    def USE_LOCAL(self) -> int:
        # Backwards compatibility with old flag (local=1, prod=0)
        return 1 if self.app_env.lower() == "local" else 0

    def as_dict(self) -> dict:
        return {
            "APP_ENV": self.app_env,
            "GOOGLE_CLOUD_PROJECT": self.gcp_project,
            "BQ_CHATBOT_ERROR_LOG": self.BQ_CHATBOT_ERROR_LOG,
            "BQ_CHATBOT_ZERO_RESULT_LOG": self.BQ_CHATBOT_ZERO_RESULT_LOG,
            "BQ_CHATBOT_QUESTION_LOG": self.BQ_CHATBOT_QUESTION_LOG,
            "BQ_CHATBOT_VOTE_FEEDBACK": self.BQ_CHATBOT_VOTE_FEEDBACK,
        }

# Singleton-style accessor if you want a quick import
CONFIG = AppConfig()
