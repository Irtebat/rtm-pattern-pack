"""Kafka connection settings.

Precedence for each value: command-line flag, then environment variable, then
AWS Secrets Manager (credentials only). Passwords are never accepted as a flag
value; use RTMPACK_SASL_PASSWORD or --sasl-password-file.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass, field
from pathlib import Path

from rtmpack_harness.contract import DEFAULT_REGION, SECRET_ID, HarnessError

ENV_BOOTSTRAP = "RTMPACK_BOOTSTRAP"
ENV_SECURITY_PROTOCOL = "RTMPACK_SECURITY_PROTOCOL"
ENV_SASL_USERNAME = "RTMPACK_SASL_USERNAME"
ENV_SASL_PASSWORD = "RTMPACK_SASL_PASSWORD"
ENV_CA_LOCATION = "RTMPACK_CA_LOCATION"
ENV_SECRET_ID = "RTMPACK_SECRET_ID"
ENV_SECRET_ARN_ALIAS = "RTMPACK_MSK_SECRET_ARN"
ENV_REGION = "RTMPACK_AWS_REGION"

SECURITY_PROTOCOLS = ("SASL_SSL", "SASL_PLAINTEXT")


@dataclass(frozen=True)
class ConnectionConfig:
    bootstrap_servers: str
    sasl_username: str
    sasl_password: str = field(repr=False)
    security_protocol: str = "SASL_SSL"
    ca_location: str | None = None

    def kafka_conf(self) -> dict[str, str]:
        """librdkafka configuration shared by Producer, Consumer and AdminClient."""
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "security.protocol": self.security_protocol,
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": self.sasl_username,
            "sasl.password": self.sasl_password,
        }
        if self.ca_location:
            conf["ssl.ca.location"] = self.ca_location
        return conf

    def describe(self) -> dict[str, str]:
        """Connection details that are safe to write to a manifest (no credentials)."""
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "security_protocol": self.security_protocol,
            "sasl_mechanism": "SCRAM-SHA-512",
        }


def add_connection_args(parser: argparse.ArgumentParser) -> None:
    group = parser.add_argument_group(
        "Kafka connection",
        "Each option falls back to the environment variable named in its help text.",
    )
    group.add_argument(
        "--bootstrap",
        default=None,
        help=f"bootstrap servers host:port,... (env {ENV_BOOTSTRAP})",
    )
    group.add_argument(
        "--security-protocol",
        choices=SECURITY_PROTOCOLS,
        default=None,
        help=f"SASL_SSL for MSK (default); SASL_PLAINTEXT only for local testing (env {ENV_SECURITY_PROTOCOL})",
    )
    group.add_argument(
        "--sasl-username",
        default=None,
        help=f"SCRAM-SHA-512 username (env {ENV_SASL_USERNAME})",
    )
    group.add_argument(
        "--sasl-password-file",
        default=None,
        help=f"file holding the SCRAM password; the password itself is read from env {ENV_SASL_PASSWORD}",
    )
    group.add_argument(
        "--ca-location",
        default=None,
        help=f"CA bundle for TLS; default is the system trust store (env {ENV_CA_LOCATION})",
    )
    group.add_argument(
        "--credentials",
        choices=("auto", "env", "secrets-manager"),
        default="auto",
        help="credential source: auto uses env or flags when both username and password are "
        "present, otherwise Secrets Manager (default: auto)",
    )
    group.add_argument(
        "--secret-id",
        default=None,
        help=f"Secrets Manager secret name or ARN (env {ENV_SECRET_ID} or {ENV_SECRET_ARN_ALIAS}; "
        f"default {SECRET_ID})",
    )
    group.add_argument(
        "--region",
        default=None,
        help=f"AWS region for Secrets Manager (env {ENV_REGION} or AWS_REGION; default {DEFAULT_REGION})",
    )


def resolve_connection(args: argparse.Namespace) -> ConnectionConfig:
    """Build a ConnectionConfig from parsed arguments, environment and Secrets Manager."""
    from rtmpack_harness.secrets import fetch_msk_credentials

    bootstrap = args.bootstrap or os.environ.get(ENV_BOOTSTRAP)
    if not bootstrap:
        raise HarnessError(f"no bootstrap servers; pass --bootstrap or set {ENV_BOOTSTRAP}")

    protocol = args.security_protocol or os.environ.get(ENV_SECURITY_PROTOCOL) or "SASL_SSL"
    if protocol not in SECURITY_PROTOCOLS:
        raise HarnessError(f"unsupported security protocol {protocol!r}")

    username = args.sasl_username or os.environ.get(ENV_SASL_USERNAME)
    password = None
    if args.sasl_password_file:
        password = Path(args.sasl_password_file).read_text(encoding="utf-8").strip()
    elif os.environ.get(ENV_SASL_PASSWORD):
        password = os.environ[ENV_SASL_PASSWORD]

    source = args.credentials
    if source == "env" or (source == "auto" and username and password):
        if not (username and password):
            raise HarnessError(
                f"credentials source 'env' needs both {ENV_SASL_USERNAME} and {ENV_SASL_PASSWORD}"
            )
    else:
        secret_id = (
            args.secret_id
            or os.environ.get(ENV_SECRET_ID)
            or os.environ.get(ENV_SECRET_ARN_ALIAS)
            or SECRET_ID
        )
        region = args.region or os.environ.get(ENV_REGION) or os.environ.get("AWS_REGION") or DEFAULT_REGION
        username, password = fetch_msk_credentials(secret_id=secret_id, region=region)

    return ConnectionConfig(
        bootstrap_servers=bootstrap,
        sasl_username=username,
        sasl_password=password,
        security_protocol=protocol,
        ca_location=args.ca_location or os.environ.get(ENV_CA_LOCATION) or None,
    )
