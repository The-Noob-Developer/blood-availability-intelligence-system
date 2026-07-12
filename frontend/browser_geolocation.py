from pathlib import Path
from typing import Any, Dict, Optional

import streamlit.components.v1 as components


_COMPONENT_DIR = Path(__file__).parent / "browser_geolocation_component"
_browser_geolocation = components.declare_component(
    "browser_geolocation",
    path=str(_COMPONENT_DIR),
)


def browser_geolocation(key: str = "browser_geolocation") -> Optional[Dict[str, Any]]:
    return _browser_geolocation(key=key, default=None)

