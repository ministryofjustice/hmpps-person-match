# ruff: noqa: DTZ001
import csv
import time
from collections import OrderedDict
from datetime import datetime

from faker import Faker

fake = Faker(["en_GB"])

Faker.seed(4321)


def stringify_list_element(datum):
    """
    Correctly formats a single element for use inside a DuckDB list constructor.
    """
    if isinstance(datum, datetime):
        datum = datum.strftime("%Y-%m-%d")
    if datum is None:
        return "NULL"  # Use the string NULL for nulls inside lists
    # Quote the string and escape any internal quotes (e.g., O'Connor -> 'O''Connor')
    return f"'{str(datum).replace("'", "''")}'"

def a_or_b(a, b, a_prob: float):
    resolution = 1_000_000_000
    return (
        a
        if fake.random_int(min=0, max=resolution) < a_prob * resolution
        else b
    )

def random_dob():
    return fake.date_between(datetime(1940, 1, 1), datetime(2007, 3, 1))

def random_id():
    return str(fake.random_int(min=1, max=5_200_000))

def random_name(name_num: int) -> str:
    missingness_prob = {
        1: 0,
        2: 0.2,
        3: 0.7,
    }
    return a_or_b(None, fake.first_name().upper(), a_prob=missingness_prob[name_num])

def fuzz_name(name: str, fuzz_letter_prob: float = 0.000_1) -> str:
    if name is None:
        return None
    # for this purpose, typos can just be a sub of an X
    return "".join(
        a_or_b("X", letter, fuzz_letter_prob)
        for letter in name
    )

def make_aliases(name) -> list[str]:
    return [fuzz_name(name)] + [fuzz_name(random_name(1)) for _ in range(fake.random_int(min=1, max=9))]

COUNTER = 0
def make_unique_person():
    global COUNTER
    if COUNTER % 10_000 == 0:
        print(f"person {COUNTER}")
    COUNTER += 1
    person_base = {
        # definitive person name
        "name_1_std": random_name(1),
        "name_2_std": random_name(2),
        "name_3_std": random_name(3),
        "last_name_std": fake.last_name().upper(),
        # a birth date
        "date_of_birth": random_dob(),
        # some postcodes
        "postcode_arr": [fake.postcode() for _ in range(fake.random_int(min=1, max=7))],
        # probably unique ids
        "cro_single": random_id(),
        "pnc_single": random_id(),
        # sentence dates
        "sentence_date_arr": [
            fake.date_between(datetime(1990, 1, 1), datetime(2025, 3, 1))
            for _ in range(fake.random_int(min=1, max=15))
        ],
    }
    person_base["aliases"] = make_aliases(person_base["name_1_std"])
    return person_base

def make_family_member(person, twin: bool = False):
    new_person = person.copy()
    # give them different forenames
    for name_num in (1, 2, 3):
        new_person[f"name_{name_num}_std"] = random_name(name_num)
    new_person["aliases"] = make_aliases(new_person["name_1_std"])
    # some different but potentially overlapping postcodes
    new_person["postcode_arr"] += [fake.postcode() for _ in range(fake.random_int(min=1, max=4))]
    new_person["postcode_arr"] = fake.random_choices(new_person["postcode_arr"], fake.random_int(min=1, max=7))
    # new IDs
    new_person["cro_single"] = a_or_b(random_id(), None, 0.2)
    new_person["pnc_single"] = a_or_b(random_id(), None, 0.1)
    if not twin:
        # and a new date of birth, if not a twin
        new_person["date_of_birth"] = random_dob()
    return new_person


def make_person_record(id_val, person_base):
    if id_val % 100_000 == 0:
        print(f"record {id_val}")
    cleaned_person = person_base.copy()
    sentence_dates = cleaned_person["sentence_date_arr"]
    sentence_dates.sort()
    # unique record details:
    cleaned_person["id"] = id_val
    cleaned_person["match_id"] = fake.uuid4()
    cleaned_person["source_system"] = fake.random_choices(["DELIUS", "NOMIS", "COMMON_PLATFORM"], length=1)[0]
    cleaned_person["source_system_id"] = fake.uuid4()
    # arbitrary marker information
    cleaned_person["override_marker"] = fake.random_elements(
        OrderedDict([
            (None, 0.999),
            (fake.uuid4(), 0.001),
        ]), length=1,
    )[0]
    cleaned_person["override_scopes"] = [fake.uuid4()] if (cleaned_person["override_marker"] is not None) else None
    #  TODO: fuzz data
    cleaned_person["name_1_std"] = fuzz_name(cleaned_person["name_1_std"])
    cleaned_person["name_2_std"] = fuzz_name(cleaned_person["name_2_std"])
    cleaned_person["name_3_std"] = fuzz_name(cleaned_person["name_3_std"])

    # Create all derived columns first, leaving them as Python lists
    cleaned_person["first_and_last_name_std"] = cleaned_person["name_1_std"] + " " + cleaned_person["last_name_std"]
    cleaned_person["forename_std_arr"] = [
        cleaned_person["name_1_std"],
        cleaned_person["name_2_std"],
        cleaned_person["aliases"],
    ]
    cleaned_person["last_name_std_arr"] = [cleaned_person["last_name_std"], fake.last_name().upper()]
    cleaned_person["date_of_birth_arr"] = [cleaned_person["date_of_birth"].strftime("%Y-%m-%d")]
    # data consistency / derived columns
    cleaned_person["postcode_outcode_arr"] = [pc.split(" ")[0] for pc in cleaned_person["postcode_arr"]]
    cleaned_person["postcode_arr"] = [pc.replace(" ", "") for pc in cleaned_person["postcode_arr"]]
    cleaned_person["sentence_date_first"] = sentence_dates[0]
    cleaned_person["sentence_date_last"] = sentence_dates[-1]
    cleaned_person["postcode_first"] = cleaned_person["postcode_arr"][0]
    cleaned_person["postcode_second"] = cleaned_person["postcode_arr"][1] if len(cleaned_person["postcode_arr"]) > 1 else None
    cleaned_person["postcode_last"] = cleaned_person["postcode_arr"][-1]
    cleaned_person["postcode_outcode_first"] = cleaned_person["postcode_outcode_arr"][0]
    cleaned_person["postcode_outcode_last"] = cleaned_person["postcode_outcode_arr"][-1]
    cleaned_person["date_of_birth_last"] = cleaned_person["date_of_birth_arr"][-1]
    cleaned_person["forename_first"] = cleaned_person["forename_std_arr"][0]
    cleaned_person["forename_last"] = cleaned_person["forename_std_arr"][-1]
    cleaned_person["last_name_first"] = cleaned_person["last_name_std_arr"][0]
    cleaned_person["last_name_last"] = cleaned_person["last_name_std_arr"][-1]

    # delete helper fields
    del cleaned_person["aliases"]

    for col, value in cleaned_person.items():
        if isinstance(value, list):
            stringified_elements = ", ".join(map(stringify_list_element, value))
            cleaned_person[col] = f"[{stringified_elements}]"  # Using DuckDB's standard array syntax
        elif isinstance(value, datetime):
            cleaned_person[col] = value.strftime("%Y-%m-%d")
        elif value is None:
            cleaned_person[col] = ""  # Represent NULL as empty string in CSV for simplicity
        else:
            cleaned_person[col] = str(value)

    return cleaned_person


n_people = 100
family_members = 10 * n_people
n_copies = 4
people = []
t1 = time.time()
base_people = [make_unique_person() for _ in range(n_people)]
people = base_people + [
    make_family_member(fake.random_element(base_people), twin=a_or_b(True, False, 0.1))
    for _ in range(family_members)
]
i = 0
records = [make_person_record(i := i + 1, person) for person in people for _ in range(n_copies)]
t2 = time.time()

print(f"Time to generate {len(records)} records: {t2 - t1}")

datafile = "dev/data/people.csv"
fieldnames = records[0].keys()

with open(datafile, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(records)

t3 = time.time()

print(f"Writing took: {t3 - t2}")
