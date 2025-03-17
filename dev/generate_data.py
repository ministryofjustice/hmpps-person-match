import csv
import time
from datetime import datetime

from faker import Faker

fake = Faker(["en_GB"])

Faker.seed(4321)

datetime(1990, 1, 1)


def make_person(id_val):
    cleaned_person = {
        "id": id_val,
        "match_id": fake.uuid4(),
        "name_1_std": fake.first_name().upper(),
        "name_2_std": fake.first_name().upper(),
        "name_3_std": fake.first_name().upper(),
        "last_name_std": fake.last_name().upper(),
        "sentence_date_single": fake.date_between(datetime(1990, 1, 1), datetime(2025, 3, 1)),
        "date_of_birth": fake.date_between(datetime(1940, 1, 1), datetime(2007, 3, 1)),
        "postcode_arr": [fake.postcode(), fake.postcode(), fake.postcode()],
        "cro_single": str(fake.random_int(min=1, max=1_000_000)),  # fake.passport_number(),
        "pnc_single": str(fake.random_int(min=1, max=650_000)),
        "crn": None,
        "prison_number": None,
        "source_system": None,
    }
    cleaned_person["first_and_last_name_std"] = cleaned_person["name_1_std"] + " " + cleaned_person["last_name_std"]
    cleaned_person["forename_std_arr"] = [cleaned_person["name_1_std"], cleaned_person["name_2_std"]]
    cleaned_person["last_name_std_arr"] = [cleaned_person["last_name_std"]]
    cleaned_person["sentence_date_arr"] = [cleaned_person["sentence_date_single"].strftime("%Y-%m-%d")]
    cleaned_person["date_of_birth_arr"] = [cleaned_person["date_of_birth"].strftime("%Y-%m-%d")]
    cleaned_person["postcode_outcode_arr"] = list(map(lambda pc: pc.split(" ")[0], cleaned_person["postcode_arr"]))
    cleaned_person["postcode_arr"] = list(map(lambda pc: pc.replace(" ", ""), cleaned_person["postcode_arr"]))

    for col in cleaned_person.keys():  # NOQA
        if isinstance(cleaned_person[col], list):
            cleaned_person[col] = "[" + ", ".join(cleaned_person[col]) + "]"

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
