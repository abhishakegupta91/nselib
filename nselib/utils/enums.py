from enum import Enum, IntEnum

class DateFormatEnum(Enum):
    """DateFormatEnum"""

    DD_MM_YYYY = "%d-%m-%Y"
    DD_MMM_YYYY = "%d-%b-%Y"
    DDMMYYYY = "%d%m%Y"
    DDMMYY = "%d%m%y"
    MMM_YY = "%b-%y"

