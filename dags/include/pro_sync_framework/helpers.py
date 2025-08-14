import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import decimal
import datetime

from airflow.models import Variable
from dateutil.parser import parse as date_parse

# IMPORTANT: Adjust the import path based on your final structure
from include.compined_kpi_national_dag.attribute_mapper import ATTRIBUTE_MAPPER
from include.pro_sync_framework.config import DagConfig
from include.pro_sync_framework.db_utils import DbHook

log = logging.getLogger(__name__)


def build_schema_from_db(config: DagConfig, db_hook: DbHook) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Dynamically builds the asset attributes and filters from the database view."""
    cols = db_hook.get_column_names(config.db_view_name)
    primary_col = config.asset_config.get("primary_name_column", "").lower()
    
    attributes: List[Dict[str, Any]] = []
    filters: List[Dict[str, Any]] = []

    for col in cols:
        if col not in ATTRIBUTE_MAPPER:
            log.warning(f"Skipping unmapped column: {col}")
            continue
        
        map_info = ATTRIBUTE_MAPPER[col]
        
        # Build attribute entry
        attr = {
            "id": col,
            "type": map_info.get("type", "string"),
            "caption": map_info.get("en", col),
            "localizations": {"caption": {"en": map_info.get("en", col), "ar": map_info.get("ar", col)}},
        }
        attributes.append(attr)

        # Create a special "name" type attribute if this is the primary display column
        if col == primary_col:
            name_attr = attr.copy()
            name_attr["id"] = f"{col}_ns" # _ns for "name string"
            name_attr["type"] = "name"
            attributes.append(name_attr)

    # Note: Filter generation logic can be added here if needed, similar to your original helper.
    # For simplicity in this refactor, it is omitted but can be easily ported.

    return attributes, filters


def build_payload(config: DagConfig, attributes: List[Dict[str, Any]], filters: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Assembles the JSON payload for creating/updating the Urbi Pro asset."""
    cfg = config.asset_config
    return {
        "name": cfg["name"],
        "description": cfg["description"],
        "geometry_dimension": cfg["geometry_dimension"],
        "localizations": {
            "name": {"en": cfg.get("name"), "ar": cfg.get("name_ar")},
            "description": {"en": cfg.get("description"), "ar": cfg.get("description_ar")},
        },
        "attribute_groups": [{
            "name": cfg.get("attribute_group_name"),
            "localizations": {"name": {"en": cfg.get("attribute_group_name"), "ar": cfg.get("attribute_group_name_ar")}},
            "attributes": attributes,
        }],
        "filters": filters,
    }

def generate_composite_pk(row_dict: Dict[str, Any], pk_columns: List[str]) -> str:
    """Creates a single unique ID string from one or more primary key columns."""
    pk_values = [str(row_dict.get(col, '')) for col in pk_columns]
    return "|".join(pk_values)


def validate_and_convert_row(row_dict: Dict[str, Any], config: DagConfig) -> Optional[Dict[str, Any]]:
    """Validates a DB row and converts it into a GeoJSON feature."""
    row_dict_lower = {k.lower(): v for k, v in row_dict.items()}
    composite_id = generate_composite_pk(row_dict_lower, config.primary_key_columns)
    
    if not composite_id:
        log.warning("Skipping record with empty composite primary key.")
        return None

    properties: Dict[str, Any] = {}
    for db_col, map_info in ATTRIBUTE_MAPPER.items():
        key = db_col.lower()
        val = row_dict_lower.get(key)
        
        if map_info.get("mandatory") and val is None:
            log.warning(f"Skipping record ID {composite_id} missing mandatory column: {db_col}")
            return None
        
        properties[key] = val

    # Sanitize data types for JSON serialization
    for k, v in properties.items():
        if isinstance(v, (datetime.datetime, datetime.date)):
            properties[k] = v.isoformat()
        elif isinstance(v, decimal.Decimal):
            properties[k] = float(v)

    # Handle primary name column for Urbi Pro's "name" type
    primary_name_col = config.asset_config.get("primary_name_column", "").lower()
    if primary_name_col and primary_name_col in properties:
        properties[f"{primary_name_col}_ns"] = properties[primary_name_col]

    # Build Geometry
    try:
        lon = float(row_dict_lower["longitude"])
        lat = float(row_dict_lower["latitude"])
        if not (-180 <= lon <= 180 and -90 <= lat <= 90):
            raise ValueError("Coordinates out of bounds.")
        geometry = {"type": "Point", "coordinates": [lon, lat]}
    except (TypeError, ValueError, KeyError) as e:
        log.warning(f"Skipping record ID {composite_id} due to invalid geometry: {e}")
        return None

    return {
        "type": "Feature",
        "id": composite_id,
        "geometry": geometry,
        "properties": properties,
    }