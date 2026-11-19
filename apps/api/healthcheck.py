from http.client import HTTPResponse
from typing import cast
from urllib.request import urlopen

with cast(HTTPResponse, urlopen("http://127.0.0.1:8000/health", timeout=2)) as response:
    if response.status != 200:
        raise SystemExit(1)
