"""
HTTP client for the ERP API.
Handles:
  - Login and bearer token acquisition (POST /api/Login)
  - Fetching documents by date (POST /api/PreAlertDoc/GetDocsByDate)
  - Saving flattened documents (POST /api/PreAlertDoc/SaveFlattenDoc)

Uses requests with SSL bypass and retry strategy to match
the production ERP server's SSL configuration.
"""
import ssl
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import config
from models import ERPDocument, SaveFlattenDocRequest

logger = logging.getLogger(__name__)


class ERPClient:
    """HTTP client for the ERP API with login-based token management"""

    def __init__(self):
        self.base_url = config.ERP_BASE_URL.rstrip("/") 
        self.email = config.ERP_EMAIL
        self.password = config.ERP_PASSWORD
        self.token = None
        self._setup_session()

    # ------------------------------------------------------------------
    # Session Setup
    # ------------------------------------------------------------------

    def _setup_session(self):
        """Configure requests session with SSL bypass and retry strategy."""
        self.session = requests.Session()
        self.session.verify = False

        # Create permissive SSL context for the ERP server
        ssl_context = None
        try:
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            ssl_context.set_ciphers("DEFAULT@SECLEVEL=0")
            ssl_context.minimum_version = ssl.TLSVersion.SSLv3
            ssl_context.maximum_version = ssl.TLSVersion.TLSv1_3
        except Exception as e:
            logger.warning(f"Could not create custom SSL context: {e}")

        # Retry strategy
        retry_strategy = Retry(
            total=5,
            backoff_factor=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "POST", "OPTIONS"],
            raise_on_status=False,
        )

        # Custom adapter with SSL context
        _ssl_ctx = ssl_context

        class _SSLAdapter(HTTPAdapter):
            def init_poolmanager(self, *args, **kwargs):
                if _ssl_ctx:
                    kwargs["ssl_context"] = _ssl_ctx
                    kwargs["assert_hostname"] = False
                return super().init_poolmanager(*args, **kwargs)

        # Plain retry adapter for HTTP (no SSL kwargs)
        http_adapter = HTTPAdapter(max_retries=retry_strategy)
        # SSL adapter for HTTPS (with SSL context + assert_hostname)
        https_adapter = _SSLAdapter(max_retries=retry_strategy)

        self.session.mount("http://", http_adapter)
        self.session.mount("https://", https_adapter)

        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) "
                "Gecko/20100101 Firefox/91.0"
            ),
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
            "Connection": "keep-alive",
        })

        logger.info("ERP session configured with SSL bypass and retry strategy")

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def login(self) -> str:
        """Authenticate with the ERP API and acquire a bearer token.

        Returns:
            The bearer token string

        Raises:
            Exception: If login fails
        """
        url = f"{self.base_url}{config.ERP_LOGIN_ENDPOINT}"
        payload = {
            "email": self.email,
            "password": self.password,
            "moduleId": 0,
            "branchId": 0,
        }

        logger.info(f"ERP login: {url}")

        try:
            response = self.session.post(url, json=payload, timeout=(10, 30))
            response.raise_for_status()

            data = response.json()
            self.token = data.get("token")

            if not self.token:
                raise Exception("Token not found in login response")

            logger.info("ERP login successful, token acquired")
            return self.token

        except requests.exceptions.SSLError as e:
            logger.error(f"ERP login SSL error: {e}")
            raise
        except requests.exceptions.ConnectionError as e:
            logger.error(f"ERP login connection error: {e}")
            raise
        except requests.exceptions.Timeout as e:
            logger.error(f"ERP login timeout: {e}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"ERP login failed: {e}")
            if hasattr(e, "response") and e.response is not None:
                logger.error(f"Status: {e.response.status_code}, Body: {e.response.text}")
            raise

    def _get_auth_headers(self) -> dict:
        """Get authorization headers, logging in if needed."""
        if not self.token:
            self.login()
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def _request_with_reauth(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make a request, re-authenticating once on 401.

        This handles token expiry: if the first request returns 401,
        we login again and retry exactly once.
        """
        headers = self._get_auth_headers()
        kwargs.setdefault("timeout", (10, 30))

        response = self.session.request(method, url, headers=headers, **kwargs)

        if response.status_code == 401:
            logger.warning("ERP returned 401 — token expired, re-authenticating")
            self.login()
            headers = self._get_auth_headers()
            response = self.session.request(method, url, headers=headers, **kwargs)

        return response

    # ------------------------------------------------------------------
    # API Methods
    # ------------------------------------------------------------------

    def fetch_documents(self, from_date: str) -> list[ERPDocument]:
        """Fetch documents from ERP that need flattening.

        Args:
            from_date: Datetime string (e.g. 2026-02-16T13:09:01.300) passed
                       as the fromDate query parameter.

        Returns:
            List of ERPDocument objects filtered to supported doc types
        """
        url = f"{self.base_url}{config.ERP_GET_DOCS_ENDPOINT}"
        params = {"fromDate": from_date}

        logger.info(f"Fetching documents from ERP: {url} (fromDate={from_date})")

        try:
            response = self._request_with_reauth("GET", url, params=params)
            response.raise_for_status()

            data = response.json()

            # Handle both list response and wrapped response
            if isinstance(data, list):
                raw_docs = data
            elif isinstance(data, dict):
                raw_docs = data.get("data", data.get("result", []))
            else:
                logger.warning(f"Unexpected response type: {type(data)}")
                return []

            documents = []
            supported_types = config.supported_doc_types()

            for raw in raw_docs:
                try:
                    doc = ERPDocument(**raw)
                except Exception as e:
                    logger.warning(f"Failed to parse document: {e}")
                    continue

                # Filter: must be a supported doc type
                if doc.docType not in supported_types:
                    continue

                # Filter: must have base64 content
                if not doc.fileBase64 or not doc.fileBase64.strip():
                    logger.warning(
                        f"Skipping lId={doc.lId} (docType={doc.docType}) — "
                        f"empty or missing fileBase64"
                    )
                    continue

                documents.append(doc)

            logger.info(
                f"Fetched {len(raw_docs)} total documents, "
                f"{len(documents)} match supported types with fileBase64"
            )
            return documents

        except requests.exceptions.RequestException as e:
            logger.error(f"ERP fetch failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching documents: {e}")
            raise

    def save_flatten_doc(self, request: SaveFlattenDocRequest) -> bool:
        """Save a flattened document back to the ERP.

        Args:
            request: SaveFlattenDocRequest with document data

        Returns:
            True on success, False on failure
        """
        url = f"{self.base_url}{config.ERP_SAVE_FLATTEN_ENDPOINT}"
        payload = request.model_dump()

        # Log payload without huge base64
        debug_payload = payload.copy()
        debug_payload["fileBase64"] = f"<base64 string ({len(payload.get('fileBase64', ''))} chars)>"
        logger.info(f"Saving flattened doc payload: {debug_payload}")

        try:
            response = self._request_with_reauth("POST", url, json=payload)

            if response.status_code in (200, 201):
                logger.info(f"ERP save successful for lId={request.lId}")
                return True

            logger.error(
                f"ERP save failed for lId={request.lId}: "
                f"{response.status_code} - {response.text}"
            )
            return False

        except requests.exceptions.RequestException as e:
            logger.error(f"ERP save request failed for lId={request.lId}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error saving lId={request.lId}: {e}")
            return False
