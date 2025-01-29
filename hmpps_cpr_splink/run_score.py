from datetime import datetime

from splink import SettingsCreator
from splink.internals.charts import waterfall_chart

from hmpps_cpr_splink.cpr_splink.model import score
from hmpps_cpr_splink.cpr_splink.schemas.joined import JoinedRecord

# TODO: probably worth maintaining some dummy data more centrally
# we can then access for test-scripts like this (and ultimatelt actual tests)
record_1 = JoinedRecord(
    id=1,
    source_system="delius",
    first_name="andrew",
    middle_names="long John",
    last_name="BOND",
    crn=None,
    prison_number=None,
    date_of_birth=datetime.strptime("2001-01-01", "%Y-%m-%d").date(),
    sex="m",
    ethnicity="W1",
    first_name_alias_arr=["andy", "drew"],
    last_name_aliases=[],
    date_of_birth_alias_arr=[],
    postcode_arr=["SW1A 1AA", "SW2 5XF"],
    cro_arr=None,
    pnc_arr=None,
    sentence_date_arr=[
        datetime.strptime("2024-11-27", "%Y-%m-%d").date(),
        datetime.strptime("2020-05-01", "%Y-%m-%d").date(),
    ],
)

record_2 = JoinedRecord(
    id=2,
    source_system="nomis",
    first_name="andy",
    middle_names="John",
    last_name="boond",
    crn=None,
    prison_number=None,
    date_of_birth=datetime.strptime("2001-11-06", "%Y-%m-%d").date(),
    sex="m",
    ethnicity="W1",
    first_name_alias_arr=["john"],
    last_name_aliases=[],
    date_of_birth_alias_arr=[],
    postcode_arr=["SW2 5XF"],
    cro_arr=None,
    pnc_arr=None,
    sentence_date_arr=[datetime.strptime("2024-11-24", "%Y-%m-%d").date()],
)

record_3 = JoinedRecord(
    id=3,
    source_system="nomis",
    first_name="andrew",
    middle_names="long John",
    last_name="BOND",
    crn=None,
    prison_number=None,
    date_of_birth=datetime.strptime("2001-01-01", "%Y-%m-%d").date(),
    sex="m",
    ethnicity="W1",
    first_name_alias_arr=["andy", "drew"],
    last_name_aliases=[],
    date_of_birth_alias_arr=[],
    postcode_arr=["SW1A 1AA", "SW2 5XF"],
    cro_arr=None,
    pnc_arr=None,
    sentence_date_arr=[
        datetime.strptime("2024-11-27", "%Y-%m-%d").date(),
        datetime.strptime("2020-05-01", "%Y-%m-%d").date(),
    ],
)


res = score(record_1, [record_2, record_3])

res.show(max_width=1000)

res = score(record_1, [record_2, record_3], return_scores_only=False)
res.show(max_width=1000)


waterfall_chart(
    res.to_arrow_table().to_pylist(),
    SettingsCreator.from_path_or_dict("model_2024_12_06_1e08.json").get_settings(
        "duckdb"
    ),
)
