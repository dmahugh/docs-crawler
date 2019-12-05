# settings.py
# https://mercadonarele.readthedocs.io/en/latest/guides/basics.html
import rele
from google.oauth2 import service_account

RELE = {
    "GC_CREDENTIALS": service_account.Credentials.from_service_account_file(
        "credentials.json"
    ),
    "GC_PROJECT_ID": "cloud-sql-docs-auditor",
}

config = rele.config.setup(RELE)
