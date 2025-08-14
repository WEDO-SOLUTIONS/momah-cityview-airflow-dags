from typing import Dict, Any

# This dictionary maps DB column names to their display properties for violation inspection data
ATTRIBUTE_MAPPER: Dict[str, Dict[str, Any]] = {

    # Core identification fields (fixed position)
    "visitnumber": {"en": "Visit Number", "ar": "رقم الزيارة", "type": "string", "mandatory": True},
    
    # Location information
    "amananame": {"en": "Amana Name", "ar": "اسم الأمانة", "type": "string", "mandatory": False},

    "longitude": {"en": "Longitude", "ar": "خط الطول", "type": "number", "mandatory": True},
    "latitude": {"en": "Latitude", "ar": "خط العرض", "type": "number", "mandatory": True},
    
    # Date information
    "createddatetime": {"en": "Date", "ar": "التاريخ", "type": "date_time", "mandatory": True},

    # Violation details
    "violationtype": {"en": "Violation Type", "ar": "نوع المخالفة", "type": "string", "mandatory": False},
    "violationamount": {"en": "Violation Amount", "ar": "مبلغ المخالفة", "type": "number", "mandatory": True}

}