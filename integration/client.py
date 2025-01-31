

from enum import Enum


class Client(str, Enum):
    """
    HMPPS Auth Clients
    Used for authentication for development
    Defined in: https://github.com/ministryofjustice/hmpps-auth/blob/main/src/main/resources/db/dev/data/auth/V900_0__clients.sql
    """
    HMPPS_TIER = "hmpps-tier" # Has roles: ROLE_COMMUNITY, ROLE_OASYS_READ_ONLY
    HMPPS_PERSON_MATCH = "hmpps-person-match" # Has roles: ROLE_PERSON_MATCH_SCORE_API__PERSON_MATCH
