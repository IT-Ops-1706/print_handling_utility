"""
Email notifications via Microsoft Graph API (OAuth2 client credentials).
Sends:
  - Per-failure alerts: immediate notification when a job permanently fails
  - Daily summary: total processed, completed, failed, in queue
"""
import logging
import httpx
import msal

from config import config

logger = logging.getLogger(__name__)


class EmailNotifier:
    """Sends email notifications via Microsoft Graph API using OAuth2"""

    GRAPH_SEND_URL = "https://graph.microsoft.com/v1.0/users/{sender}/sendMail"

    def __init__(self):
        self.tenant_id = config.EMAIL_TENANT_ID
        self.client_id = config.EMAIL_CLIENT_ID
        self.client_secret = config.EMAIL_CLIENT_SECRET
        self.sender = config.EMAIL_SENDER
        self.to_list = config.email_to_list()
        self.cc_list = config.email_cc_list()
        self._token_cache = None

    def _get_access_token(self) -> str | None:
        """Acquire an access token using MSAL client credentials flow"""
        try:
            authority = f"https://login.microsoftonline.com/{self.tenant_id}"
            app = msal.ConfidentialClientApplication(
                self.client_id,
                authority=authority,
                client_credential=self.client_secret,
            )

            result = app.acquire_token_for_client(
                scopes=["https://graph.microsoft.com/.default"]
            )

            if "access_token" in result:
                return result["access_token"]

            logger.error(f"Failed to acquire token: {result.get('error_description', 'Unknown')}")
            return None

        except Exception as e:
            logger.error(f"Token acquisition failed: {e}")
            return None

    def _build_recipients(self, addresses: list[str]) -> list[dict]:
        """Build Graph API recipient format"""
        return [
            {"emailAddress": {"address": addr}}
            for addr in addresses
            if addr
        ]

    def _send_email(self, subject: str, body_html: str) -> bool:
        """Send an email via Microsoft Graph API.

        Returns:
            True on success, False on failure
        """
        if not self.to_list:
            logger.warning("No EMAIL_TO configured, skipping email")
            return False

        token = self._get_access_token()
        if not token:
            return False

        url = self.GRAPH_SEND_URL.format(sender=self.sender)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        payload = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": body_html,
                },
                "toRecipients": self._build_recipients(self.to_list),
            },
            "saveToSentItems": "false",
        }

        # Add CC if configured
        if self.cc_list:
            payload["message"]["ccRecipients"] = self._build_recipients(self.cc_list)

        try:
            response = httpx.post(url, headers=headers, json=payload, timeout=30)
            if response.status_code == 202:
                logger.info(f"Email sent: {subject}")
                return True
            else:
                logger.error(f"Email send failed: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            logger.error(f"Email send error: {e}")
            return False

    # ------------------------------------------------------------------
    # Public Methods
    # ------------------------------------------------------------------

    def send_failure_alert(
        self, lId: int, fileName: str, error: str, error_type: str, retry_count: int
    ) -> bool:
        """Send immediate notification for a permanently failed job"""
        subject = f"[FLATTEN FAILED] lId={lId} — {fileName}"
        body = f"""
        <h3 style="color:#c0392b;">Flatten Job Failed (Permanent)</h3>
        <table style="border-collapse:collapse; font-family:monospace;">
            <tr><td style="padding:4px 12px;"><b>Document ID</b></td><td>{lId}</td></tr>
            <tr><td style="padding:4px 12px;"><b>File Name</b></td><td>{fileName}</td></tr>
            <tr><td style="padding:4px 12px;"><b>Error Type</b></td><td>{error_type}</td></tr>
            <tr><td style="padding:4px 12px;"><b>Error</b></td><td>{error}</td></tr>
            <tr><td style="padding:4px 12px;"><b>Retry Count</b></td><td>{retry_count}</td></tr>
        </table>
        <p style="color:#7f8c8d;font-size:12px;">Flatten Orchestrator — Automated Alert</p>
        """
        return self._send_email(subject, body)

    def send_daily_report(self, stats: dict) -> bool:
        """Send daily summary report.

        Args:
            stats: dict with keys: total_processed, completed, failed,
                   in_queue, pending_retry, permanently_failed
        """
        subject = f"[FLATTEN REPORT] {stats.get('total_processed', 0)} processed — {stats.get('failed', 0)} failed"

        # Build failed jobs table rows
        failed_rows = ""
        for job in stats.get("failed_details", []):
            failed_rows += f"""
            <tr>
                <td style="padding:4px 8px;border:1px solid #ddd;">{job.get('lId', '')}</td>
                <td style="padding:4px 8px;border:1px solid #ddd;">{job.get('fileName', '')}</td>
                <td style="padding:4px 8px;border:1px solid #ddd;">{job.get('error_type', '')}</td>
                <td style="padding:4px 8px;border:1px solid #ddd;">{job.get('error', '')}</td>
            </tr>
            """

        failed_table = ""
        if failed_rows:
            failed_table = f"""
            <h4 style="color:#c0392b;">Failed Jobs</h4>
            <table style="border-collapse:collapse; font-family:monospace; width:100%;">
                <tr style="background:#f2f2f2;">
                    <th style="padding:6px 8px;border:1px solid #ddd;text-align:left;">lId</th>
                    <th style="padding:6px 8px;border:1px solid #ddd;text-align:left;">File</th>
                    <th style="padding:6px 8px;border:1px solid #ddd;text-align:left;">Error Type</th>
                    <th style="padding:6px 8px;border:1px solid #ddd;text-align:left;">Error</th>
                </tr>
                {failed_rows}
            </table>
            """

        body = f"""
        <h3>Flatten Orchestrator — Daily Report</h3>
        <table style="border-collapse:collapse; font-family:monospace;">
            <tr><td style="padding:4px 12px;"><b>Total Processed</b></td><td>{stats.get('total_processed', 0)}</td></tr>
            <tr><td style="padding:4px 12px;"><b>Completed</b></td><td style="color:#27ae60;">{stats.get('completed', 0)}</td></tr>
            <tr><td style="padding:4px 12px;"><b>Failed</b></td><td style="color:#c0392b;">{stats.get('failed', 0)}</td></tr>
            <tr><td style="padding:4px 12px;"><b>In Queue</b></td><td>{stats.get('in_queue', 0)}</td></tr>
            <tr><td style="padding:4px 12px;"><b>Pending Retry</b></td><td>{stats.get('pending_retry', 0)}</td></tr>
            <tr><td style="padding:4px 12px;"><b>Permanently Failed</b></td><td>{stats.get('permanently_failed', 0)}</td></tr>
        </table>
        {failed_table}
        <p style="color:#7f8c8d;font-size:12px;">Flatten Orchestrator — Automated Report</p>
        """
        return self._send_email(subject, body)

    def is_configured(self) -> bool:
        """Check if email is properly configured"""
        return all([
            self.tenant_id,
            self.client_id,
            self.client_secret,
            self.sender,
            self.to_list,
        ])
