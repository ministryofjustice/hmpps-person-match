import datetime
import random
import string
import uuid


def random_match_id() -> str:
    return str(uuid.uuid4())


def random_name() -> str:
    return random_lower_case_string()


def random_crn() -> str:
    return random_lower_case_string(length=1).upper() + random_digit(length=6)


def random_prison_number() -> str:
    return random_lower_case_string(1).upper() + random_digit(4) + random_lower_case_string(2).upper()


def random_defendant_id() -> str:
    return str(uuid.uuid4())


def random_c_id() -> str:
    return random_digit(9)


def random_date() -> str:
    return datetime.date(random_year(), random.randint(1, 12), random.randint(1, 28)).strftime(  # noqa: S311
        "%Y-%m-%d",
    )


def random_digit(length=7) -> str:
    return "".join(str(random.randint(0, 9)) for _ in range(length))  # noqa: S311


def random_lower_case_string(length=7) -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(length))  # noqa: S311


def random_year() -> int:
    return random.randint(1950, datetime.datetime.now().astimezone().year)  # noqa: S311


def random_postcode() -> str:
    return (
        random_lower_case_string(1).upper()
        + random_digit(2)
        + " "
        + random_digit(1)
        + random_lower_case_string(2).upper()
    )


def random_cro() -> str:
    return str(random_year())[2:] + "/" + random_digit(6) + random_lower_case_string(1).upper()


def random_pnc() -> str:
    return str(random_year())[2:] + "/" + random_digit(7) + random_lower_case_string(1).upper()


def random_source_system() -> str:
    systems = ["NOMIS", "DELIUS", "LIBRA", "COMMON_PLATFORM"]
    return random.choice(systems)  # noqa: S311


def random_source_system_id() -> str:
    ids = [random_crn, random_prison_number, random_defendant_id, random_c_id]
    return random.choice(ids)()  # noqa: S311
