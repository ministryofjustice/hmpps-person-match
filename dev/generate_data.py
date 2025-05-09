import csv
import time
from datetime import datetime

from faker import Faker

fake = Faker(["en_GB"])

Faker.seed(4321)


def stringify(datum):
    if isinstance(datum, datetime):
        return datum.strftime("%Y-%m-%d")
    return str(datum)


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
        "source_system": None,
        "source_system_id": None,
    }
    # NB: not completely consistent with real data - no duplicates, arrays not sorted
    cleaned_person["first_and_last_name_std"] = cleaned_person["name_1_std"] + " " + cleaned_person["last_name_std"]
    cleaned_person["forename_std_arr"] = [
        cleaned_person["name_1_std"],
        cleaned_person["name_2_std"],
        fake.first_name().upper(),
    ]
    cleaned_person["last_name_std_arr"] = [cleaned_person["last_name_std"], fake.last_name().upper()]
    cleaned_person["date_of_birth_arr"] = [cleaned_person["date_of_birth"].strftime("%Y-%m-%d")]
    cleaned_person["postcode_outcode_arr"] = list(map(lambda pc: pc.split(" ")[0], cleaned_person["postcode_arr"]))
    cleaned_person["postcode_arr"] = list(map(lambda pc: pc.replace(" ", ""), cleaned_person["postcode_arr"]))
    # derived columns
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

    for col in cleaned_person.keys():  # NOQA
        if isinstance(cleaned_person[col], list):
            cleaned_person[col] = "[" + ", ".join(map(stringify, cleaned_person[col])) + "]"

    return cleaned_person


n_people = 3_500_000
people = []
t1 = time.time()
for i in range(n_people):
    people.append(make_person(i))
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
