from dataclasses import dataclass


@dataclass(frozen=True)
class S3Config:
    url: str
    access_key: str
    secret_key: str



