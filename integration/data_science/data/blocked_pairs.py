# pairs of records that should be blocked by our rules
import itertools
from dataclasses import asdict, dataclass, field
from datetime import date


@dataclass
class CleanedPerson:
    _id_sequence = itertools.count()

    source_system: str
    name_1_std: str
    name_2_std: str
    name_3_std: str
    last_name_std: str
    first_and_last_name_std: str
    forename_std_arr: list[str]
    last_name_std_arr: list[str]
    date_of_birth: date
    date_of_birth_arr: list[date]
    sentence_date_single: date
    sentence_date_arr: list[date]
    postcode_arr: list[str]
    postcode_outcode_arr: list[str]
    cro_single: str
    pnc_single: str
    match_id: str = field(default_factory=lambda:f"match_{next(CleanedPerson._id_sequence)}")
    id: int = field(default_factory=lambda: next(CleanedPerson._id_sequence))

    def as_dict(self) -> dict:
        return asdict(self)

@dataclass
class RecordPair:
    record_1: CleanedPerson
    record_2: CleanedPerson
    reason_for_passing_blocking: str
    # match_key tells us which was first blocking rule that allowed pair to pass
    expected_match_key: str

    @property
    def records(self) -> list[CleanedPerson]:
        return self.record_1, self.record_2

blocked_pairs = [
    RecordPair(
        CleanedPerson(
            source_system="DELIUS",
            name_1_std="ANDY",
            name_2_std=None,
            name_3_std=None,
            last_name_std="BOND",
            first_and_last_name_std="ANDY BOND",
            forename_std_arr=("ANDY",),
            last_name_std_arr=("BOND",),
            date_of_birth=date(1980, 1, 1),
            date_of_birth_arr=(date(1980, 1, 1),),
            sentence_date_single=date(2025, 2, 19),
            sentence_date_arr=(date(2025, 2, 19),),
            postcode_arr=("AB11AB",),
            postcode_outcode_arr=("AB",),
            cro_single="CRO1",
            pnc_single="PNC1",
        ),
        CleanedPerson(
            source_system="DELIUS",
            name_1_std="JOHN",
            name_2_std=None,
            name_3_std=None,
            last_name_std="SMITH",
            first_and_last_name_std="JOHN SMITH",
            forename_std_arr=("JOHN",),
            last_name_std_arr=("SMITH",),
            date_of_birth=date(1970, 1, 1),
            date_of_birth_arr=(date(1970, 1, 1),),
            sentence_date_single=date(2023, 2, 19),
            sentence_date_arr=(date(2023, 2, 19),),
            postcode_arr=("ZX99XZ",),
            postcode_outcode_arr=("ZX",),
            cro_single="CRO2",
            pnc_single="PNC1",
        ),
        "PNC matches",
        expected_match_key="0",
    ),
    RecordPair(
        CleanedPerson(
            source_system="DELIUS",
            name_1_std="ANDY",
            name_2_std=None,
            name_3_std=None,
            last_name_std="BOND",
            first_and_last_name_std="ANDY BOND",
            forename_std_arr=("ANDY",),
            last_name_std_arr=("BOND",),
            date_of_birth=date(1980, 1, 1),
            date_of_birth_arr=(date(1980, 1, 1),),
            sentence_date_single=date(2025, 2, 19),
            sentence_date_arr=(date(2025, 2, 19),),
            postcode_arr=("AB11AB",),
            postcode_outcode_arr=("AB",),
            cro_single="CRO1",
            pnc_single="PNC1",
        ),
        CleanedPerson(
            source_system="DELIUS",
            name_1_std="JOHN",
            name_2_std=None,
            name_3_std=None,
            last_name_std="SMITH",
            first_and_last_name_std="JOHN SMITH",
            forename_std_arr=("JOHN",),
            last_name_std_arr=("SMITH",),
            date_of_birth=date(1970, 1, 1),
            date_of_birth_arr=(date(1970, 1, 1),),
            sentence_date_single=date(2023, 2, 19),
            sentence_date_arr=(date(2023, 2, 19),),
            postcode_arr=("ZX99XZ",),
            postcode_outcode_arr=("ZX",),
            cro_single="CRO1",
            pnc_single="PNC2",
        ),
        "CRO matches",
        expected_match_key="1",
    ),
    RecordPair(
        CleanedPerson(
            source_system="DELIUS",
            name_1_std="ANDY",
            name_2_std=None,
            name_3_std=None,
            last_name_std="BOND",
            first_and_last_name_std="ANDY BOND",
            forename_std_arr=("ANDY",),
            last_name_std_arr=("BOND",),
            date_of_birth=date(1980, 1, 1),
            date_of_birth_arr=(date(1980, 1, 1),),
            sentence_date_single=date(2025, 2, 19),
            sentence_date_arr=(date(2025, 2, 19),),
            postcode_arr=("AB11AB", "AA33AA"),
            postcode_outcode_arr=("AA",),
            cro_single="CRO1",
            pnc_single="PNC1",
        ),
        CleanedPerson(
            source_system="DELIUS",
            name_1_std="JOHN",
            name_2_std=None,
            name_3_std=None,
            last_name_std="SMITH",
            first_and_last_name_std="JOHN SMITH",
            forename_std_arr=("JOHN",),
            last_name_std_arr=("SMITH",),
            date_of_birth=date(1980, 1, 1),
            date_of_birth_arr=(date(1970, 1, 1),),
            sentence_date_single=date(2023, 2, 19),
            sentence_date_arr=(date(2023, 2, 19),),
            postcode_arr=("AB11AB", "ZX99XZ"),
            postcode_outcode_arr=("ZX",),
            cro_single="CRO2",
            pnc_single="PNC2",
        ),
        "DOB + first postcode matches",
        expected_match_key="2",
    ),
    RecordPair(
        CleanedPerson(
            source_system="DELIUS",
            name_1_std="ANDY",
            name_2_std=None,
            name_3_std=None,
            last_name_std="BOND",
            first_and_last_name_std="ANDY BOND",
            forename_std_arr=("ANDY",),
            last_name_std_arr=("BOND",),
            date_of_birth=date(1980, 1, 1),
            date_of_birth_arr=(date(1980, 1, 1),),
            sentence_date_single=date(2025, 2, 19),
            sentence_date_arr=(date(2025, 2, 19),),
            postcode_arr=("AB11AB", ),
            postcode_outcode_arr=("ZX", "AB", "ZW"),
            cro_single="CRO1",
            pnc_single="PNC1",
        ),
        CleanedPerson(
            source_system="DELIUS",
            name_1_std="ANNIE",
            name_2_std=None,
            name_3_std=None,
            last_name_std="SMITH",
            first_and_last_name_std="ANNIE SMITH",
            forename_std_arr=("ANNIE",),
            last_name_std_arr=("SMITH",),
            date_of_birth=date(1980, 1, 1),
            date_of_birth_arr=(date(1970, 1, 1),),
            sentence_date_single=date(2023, 2, 19),
            sentence_date_arr=(date(2023, 2, 19),),
            postcode_arr=("ZX99XZ", ),
            postcode_outcode_arr=("ZX", "ZZ", "AA", "ZW"),
            cro_single="CRO2",
            pnc_single="PNC2",
        ),
        "DOB, first outcode + first two name_1 characters match",
        expected_match_key="3",
    ),
    RecordPair(
        CleanedPerson(
            source_system="DELIUS",
            name_1_std="ANDY",
            name_2_std=None,
            name_3_std=None,
            last_name_std="SMOND",
            first_and_last_name_std="ANDY SMOND",
            forename_std_arr=("ANDY",),
            last_name_std_arr=("SMOND",),
            date_of_birth=date(1980, 1, 1),
            date_of_birth_arr=(date(1980, 1, 1), date(1970, 4, 4)),
            sentence_date_single=date(2025, 2, 19),
            sentence_date_arr=(date(2025, 2, 19),),
            postcode_arr=("AB11AB", ),
            postcode_outcode_arr=("AA", "AB", "ZW"),
            cro_single="CRO1",
            pnc_single="PNC1",
        ),
        CleanedPerson(
            source_system="DELIUS",
            name_1_std="JOHN",
            name_2_std=None,
            name_3_std=None,
            last_name_std="SMITH",
            first_and_last_name_std="JOHN SMITH",
            forename_std_arr=("JOHN",),
            last_name_std_arr=("SMITH",),
            date_of_birth=date(1970, 1, 1),
            date_of_birth_arr=(date(1970, 1, 1), date(1970, 3, 3), date(1970, 4, 4)),
            sentence_date_single=date(2023, 2, 19),
            sentence_date_arr=(date(2023, 2, 19),),
            postcode_arr=("ZX99XZ", ),
            postcode_outcode_arr=("ZX", "ZZ", "AA", "ZW"),
            cro_single="CRO2",
            pnc_single="PNC2",
        ),
        "Final DOB, final outcode + first two surname characters match",
        expected_match_key="4",
    ),
]
