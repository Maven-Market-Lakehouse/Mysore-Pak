import os
import re
from typing import Any, Dict

import yaml


_TOKEN_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _resolve_token(token: str, config: Dict[str, Any]) -> str:
    if token.startswith("secrets."):
        key = token.split(".", 1)[1]
        return f"{{{{secrets/{key}}}}}"

    parts = token.split(".")
    current: Any = config
    for part in parts:
        if not isinstance(current, dict) or part not in current:
            raise KeyError(f"Unable to resolve config token: {token}")
        current = current[part]
    return str(current)


def _resolve_node(node: Any, root: Dict[str, Any]) -> Any:
    if isinstance(node, dict):
        return {k: _resolve_node(v, root) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve_node(item, root) for item in node]
    if isinstance(node, str):
        def replacer(match: re.Match) -> str:
            return _resolve_token(match.group(1), root)

        return _TOKEN_PATTERN.sub(replacer, node)
    return node


def load_config(config_path: str = None) -> Dict[str, Any]:
    if config_path is None:
        config_path = os.environ.get("MAVEN_CONFIG_PATH", "config/config.yml")

    with open(config_path, "r", encoding="utf-8") as config_file:
        raw_config: Dict[str, Any] = yaml.safe_load(config_file)

    return _resolve_node(raw_config, raw_config)
