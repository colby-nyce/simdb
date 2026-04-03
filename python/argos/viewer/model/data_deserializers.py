# Argos collection-record blob decoding. Expanded incrementally alongside validate.py.
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from viewer.model.dtype_inspector import DataTypeInspector


def build_deserializer(inspector, dtype_name):
    # type: (DataTypeInspector, str) -> Optional[DataDeserializer]
    """Return a deserializer for dtype_name, or None if not implemented yet."""
    return None


class DataDeserializer:
    """Base type for decoders that turn raw Argos record bytes into Python values."""

    pass
