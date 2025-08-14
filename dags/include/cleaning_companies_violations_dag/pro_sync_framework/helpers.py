import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import decimal
import datetime

from airflow.models import Variable
from dateutil.parser import parse as date_parse

# IMPORTANT: Adjust the import path based on your final structure
from attribute_mapper import ATTRIBUTE_MAPPER
from config import DagConfig
from db_utils import DbHook

log = logging.getLogger(__name__)


def build_schema_from_db(config: DagConfig, db_hook: DbHook) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Dynamically builds the asset attributes and filters from the database view."""
    log.info(f"Building schema from database view: {config.db_view_name}")
    
    # Get the raw column description first to preserve original casing for SQL
    with db_hook.hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {config.db_view_name} WHERE 1=0")
            # raw_cols will be like ['DATETIME', 'MUNICIPALITY_ID', 'VPI']
            raw_cols = [desc[0] for desc in cur.description]
    
    # cols will be like ['datetime', 'municipality_id', 'vpi']
    cols = [c.lower() for c in raw_cols]
    
    primary_col = config.asset_config.get("primary_name_column", "").lower()
    
    attributes: List[Dict[str, Any]] = []
    filters: List[Dict[str, Any]] = []

    MIN_DATE_MS = 315522000000
    MAX_DATE_MS = 2524597200000

    for raw_col, col in zip(raw_cols, cols):
        if col not in ATTRIBUTE_MAPPER:
            log.warning(f"Skipping unmapped column: {col}")
            continue
        
        map_info = ATTRIBUTE_MAPPER[col]
        
        attr = {
            "id": col,
            "type": map_info.get("type", "string"),
            "caption": map_info.get("en", col),
            "localizations": {"caption": {"en": map_info.get("en", col), "ar": map_info.get("ar", col)}},
        }
        attributes.append(attr)

        if col == primary_col:
            name_attr = attr.copy()
            name_attr["id"] = f"{col}_ns"
            name_attr["type"] = "name"
            attributes.append(name_attr)

        if col in ("latitude", "longitude"): # or col in config.primary_key_columns:
            continue

        filter_obj: Dict[str, Any] = {"attribute_id": col}
        try:
            col_type = map_info.get("type", "string")
            
            # ✨ FIX: Use the 'raw_col' with original casing in all SQL queries
            if col_type == "string":
                count_sql = f'SELECT COUNT(DISTINCT "{raw_col}") FROM {config.db_view_name}'
                distinct_count = db_hook.hook.get_first(count_sql)[0] or 0
                
                if 0 < distinct_count <= 50:
                    filter_obj["control_type"] = "check_box_list"
                    items_sql = f'SELECT DISTINCT "{raw_col}" FROM {config.db_view_name} WHERE "{raw_col}" IS NOT NULL'
                    rows = db_hook.hook.get_records(items_sql)
                    filter_obj["items"] = [{"value": str(r[0]), "caption": str(r[0])} for r in rows]
                else:
                    filter_obj["control_type"] = "text_box"

            elif col_type == "date_time":
                filter_obj.update({"control_type": "date_time_range", "min": MIN_DATE_MS, "max": MAX_DATE_MS})

            elif col_type == "number":
                range_sql = f'SELECT MIN("{raw_col}"), MAX("{raw_col}") FROM {config.db_view_name}'
                min_val, max_val = db_hook.hook.get_first(range_sql)
                
                if min_val is not None and max_val is not None:
                    filter_obj.update({"control_type": "range", "min": float(min_val), "max": float(max_val)})
                else:
                    continue
            
            else:
                continue
        
        except Exception as e:
            log.warning(f"Could not generate filter for column '{col}'. Defaulting to text box. Error: {e}")
            filter_obj["control_type"] = "text_box"

        filters.append(filter_obj)

    log.info(f"Generated {len(attributes)} attributes and {len(filters)} filters.")
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