import csv
import random
import time
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


def make_person(id_val):
    sentence_dates = [
        fake.date_between(datetime(1990, 1, 1), datetime(2025, 3, 1)),  # noqa: DTZ001
        fake.date_between(datetime(1990, 1, 1), datetime(2025, 3, 1)),  # noqa: DTZ001
    ]
    sentence_dates.sort()
    cleaned_person = {
        "id": id_val,
        "match_id": fake.uuid4(),
        "name_1_std": fake.first_name().upper(),
        "name_2_std": fake.first_name().upper(),
        "name_3_std": fake.first_name().upper(),
        "last_name_std": fake.last_name().upper(),
        "sentence_date_arr": sentence_dates,
        "date_of_birth": fake.date_between(datetime(1940, 1, 1), datetime(2007, 3, 1)),  # noqa: DTZ001
        "postcode_arr": [fake.postcode(), fake.postcode(), fake.postcode()],
        "cro_single": str(fake.random_int(min=1, max=1_000_000)),  # fake.passport_number(),
        "pnc_single": str(fake.random_int(min=1, max=650_000)),
        "source_system": fake.random_element(["DELIUS", "NOMIS"]),
        "source_system_id": fake.uuid4(),
        "override_marker": None,
        "override_scopes": None,
    }

    # Create all derived columns first, leaving them as Python lists
    cleaned_person["first_and_last_name_std"] = cleaned_person["name_1_std"] + " " + cleaned_person["last_name_std"]
    cleaned_person["forename_std_arr"] = [
        cleaned_person["name_1_std"],
        cleaned_person["name_2_std"],
        fake.first_name().upper(),
    ]
    cleaned_person["last_name_std_arr"] = [cleaned_person["last_name_std"], fake.last_name().upper()]
    cleaned_person["date_of_birth_arr"] = [cleaned_person["date_of_birth"].strftime("%Y-%m-%d")]
    cleaned_person["postcode_outcode_arr"] = [pc.split(" ")[0] for pc in cleaned_person["postcode_arr"]]
    cleaned_person["postcode_arr"] = [pc.replace(" ", "") for pc in cleaned_person["postcode_arr"]]
    cleaned_person["sentence_date_first"] = sentence_dates[0]
    cleaned_person["sentence_date_last"] = sentence_dates[-1]
    cleaned_person["postcode_first"] = cleaned_person["postcode_arr"][0]
    cleaned_person["postcode_second"] = cleaned_person["postcode_arr"][1]
    cleaned_person["postcode_last"] = cleaned_person["postcode_arr"][-1]
    cleaned_person["postcode_outcode_first"] = cleaned_person["postcode_outcode_arr"][0]
    cleaned_person["postcode_outcode_last"] = cleaned_person["postcode_outcode_arr"][-1]
    cleaned_person["date_of_birth_last"] = cleaned_person["date_of_birth_arr"][-1]
    cleaned_person["forename_first"] = cleaned_person["forename_std_arr"][0]
    cleaned_person["forename_last"] = cleaned_person["forename_std_arr"][-1]
    cleaned_person["last_name_first"] = cleaned_person["last_name_std_arr"][0]
    cleaned_person["last_name_last"] = cleaned_person["last_name_std_arr"][-1]

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


n_people = 3_500_0
people = []
t1 = time.time()

next_id = 0

while len(people) < n_people:
    base_person = make_person(0)
    copies = random.randint(1, 5)  # noqa: S311
    copies = min(copies, n_people - len(people))

    for _ in range(copies):
        person = base_person.copy()
        person["id"] = str(next_id)
        person["match_id"] = fake.uuid4()
        person["source_system_id"] = fake.uuid4()
        people.append(person)
        next_id += 1
t2 = time.time()

print(f"Time is {t2 - t1}")
print(f"For all, we have {(t2 - t1) * 3_500_000 / n_people}")

datafile = "dev/data/people.csv"
fieldnames = people[0].keys()

with open(datafile, "w", newline="") as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(people)

t3 = time.time()

print(f"Writing took: {t3 - t2}")
