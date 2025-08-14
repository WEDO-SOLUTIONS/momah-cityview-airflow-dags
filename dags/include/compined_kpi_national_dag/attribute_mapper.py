from typing import Dict, Any

# This dictionary maps DB column names to their display properties for street inspection data
ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = { 

    # Core date and location fields (fixed position)
    "datetime": {"en": "Date", "ar": "التاريخ", "type": "date_time", "mandatory": True},

    "latitude": {"en": "Latitude", "ar": "خط العرض", "type": "number", "mandatory": True},
    "longitude": {"en": "Longitude", "ar": "خط الطول", "type": "number", "mandatory": True},

    "amana_name_ar": {"en": "Amana Name AR", "ar": "اسم الأمانة بالعربية", "type": "string", "mandatory": False},

    # Administrative information
    "municipality_id": {"en": "Municipality ID", "ar": "معرف البلدية", "type": "string", "mandatory": False},
    
    # Area measurements
    "streetarea": {"en": "Total Street Area", "ar": "إجمالي مساحة الشوارع", "type": "string", "mandatory": False},
    "inspectedstreetarea": {"en": "Inspected Street Area", "ar": "مساحة الشوارع المغطاه", "type": "string", "mandatory": False},

    # Inspection metrics
    "units": {"en": "Units", "ar": "الوحدات", "type": "string", "mandatory": False},
    "cov": {"en": "Coverage", "ar": "التغطية", "type": "number", "mandatory": True},
    "vpi": {"en": "VP Index", "ar": "مؤشر التشوه البصري", "type": "number", "mandatory": True}

}