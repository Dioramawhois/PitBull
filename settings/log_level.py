from enum import IntEnum


class LogLevelEnum(IntEnum):
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50

    @classmethod
    def _missing_(cls, value):
        if isinstance(value, str):
            value_upper = value.upper()
            for member in cls:
                if member.name == value_upper:
                    return member
        return super()._missing_(value)
