import os
import re

from typing_extensions import List, Union

# Matches environment variable placeholders like ${VAR_NAME}
_ENV_PATTERN = re.compile(r"\$\{(\w+)\}")


def find_env(value: str) -> List[object]:
    """Return all environment variable names found in a string."""
    return _ENV_PATTERN.findall(value)


def replace_env(value: Union[dict, list, str]) -> Union[dict, list, str]:
    """
    Recursively replace ${VAR_NAME} placeholders with actual environment
    variable values. Non-string values are returned as-is.
    """
    if isinstance(value, dict):
        return {k: replace_env(v) for k, v in value.items()}

    if isinstance(value, list):
        return [replace_env(v) for v in value]

    if isinstance(value, str):
        match = _ENV_PATTERN.search(value)
        if match:
            return os.getenv(match.group(1), value)

    return value


def clean_multiple_line(value: str) -> str:
    """Collapse a multi-line string into a single space-separated line."""
    return value.replace("\n", " ").strip()
