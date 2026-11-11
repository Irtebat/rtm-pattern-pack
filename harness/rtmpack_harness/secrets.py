"""Read the MSK SCRAM credentials from AWS Secrets Manager.

The secret is JSON of the form {"username": "...", "password": "..."}.
boto3 honours the usual AWS_PROFILE, AWS_REGION and instance-role settings.
"""

from __future__ import annotations

import json
from typing import Any

from rtmpack_harness.contract import DEFAULT_REGION, SECRET_ID, HarnessError


def fetch_msk_credentials(
    secret_id: str = SECRET_ID,
    region: str = DEFAULT_REGION,
    client: Any = None,
) -> tuple[str, str]:
    """Return (username, password) from the named secret.

    `client` can be a Secrets Manager client (used by tests); by default one is
    created with boto3 for `region`.
    """
    if client is None:
        try:
            import boto3
        except ImportError as exc:  # pragma: no cover - boto3 is a declared dependency
            raise HarnessError("boto3 is required to read Secrets Manager") from exc
        client = boto3.session.Session(region_name=region).client("secretsmanager")

    try:
        response = client.get_secret_value(SecretId=secret_id)
    except Exception as exc:  # botocore raises several exception types
        raise HarnessError(f"cannot read secret {secret_id!r} in {region}: {exc}") from exc

    raw = response.get("SecretString")
    if not raw:
        raise HarnessError(f"secret {secret_id!r} has no SecretString value")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HarnessError(f"secret {secret_id!r} is not JSON") from exc

    username = data.get("username")
    password = data.get("password")
    if not username or not password:
        raise HarnessError(f"secret {secret_id!r} must contain 'username' and 'password'")
    return str(username), str(password)
