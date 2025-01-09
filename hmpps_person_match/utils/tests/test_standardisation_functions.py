import datetime

import pandas as pd

from hmpps_person_match.utils.standardisation_functions import (
    fix_zero_length_strings,
    null_suspicious_dob_std,
    standardise_dob,
    standardise_names,
    standardise_pnc_number,
)


class TestStandardisationFunctions:
    """
    Test standardisation function
    """

    def test_dob(self):
        """
        Test date of birth standardisation
        """
        dt = datetime.datetime(2020, 5, 25, 8, 5, 44, 815715)
        date = dt.date()
        date_str = date.strftime("%Y-%m-%d")
        date_str_alt = date.strftime("%d/%m/%Y")

        names_list = [
            {
                "A": dt,
                "B": date,
                "C": date_str,
                "D": date_str_alt,
                "E": None,
                "F": None,
                "G": None,
            },
        ]

        df = pd.DataFrame(names_list)

        expected = [
            {"dob_std": "2020-05-25"},
            {"dob_std": "2020-05-25"},
            {"dob_std": "2020-05-25"},
            {"dob_std": "2020-05-25"},
            {"dob_std": None},
            {"dob_std": None},
            {"dob_std": None},
        ]
        df_expected = pd.DataFrame(expected)

        df_result = standardise_dob(df[["A"]], "A")
        pd.testing.assert_frame_equal(df_result, df_expected.iloc[[0], :].reset_index(drop=True))

        df_result = standardise_dob(df[["B"]], "B")
        pd.testing.assert_frame_equal(df_result, df_expected.iloc[[1], :].reset_index(drop=True))

        df_result = standardise_dob(df[["C"]], "C")
        pd.testing.assert_frame_equal(df_result, df_expected.iloc[[2], :].reset_index(drop=True))

        df_result = standardise_dob(df[["D"]], "D")
        pd.testing.assert_frame_equal(df_result, df_expected.iloc[[3], :].reset_index(drop=True))

        df_result = standardise_dob(df[["E"]], "E")
        pd.testing.assert_frame_equal(df_result, df_expected.iloc[[4], :].reset_index(drop=True))

        df_result = standardise_dob(df[["F"]], "F")
        pd.testing.assert_frame_equal(df_result, df_expected.iloc[[5], :].reset_index(drop=True))

        df_result = standardise_dob(df[["G"]], "G")
        pd.testing.assert_frame_equal(df_result, df_expected.iloc[[6], :].reset_index(drop=True))

        # Unsure this is the best way of handling.  Should we be nulling instead of raising an error.
        # What about people who were actually born 1970-01-01?

    def test_null_suspicious_dob_std(self):
        """
        Test null date of birth standardisation
        """
        names_list = [{"A": "1900-01-01", "B": "1970-01-01", "C": None}]

        df = pd.DataFrame(names_list)

        expected = [{"dob_std": None}, {"dob_std": None}, {"dob_std": None}]
        df_expected = pd.DataFrame(expected)

        df_result = null_suspicious_dob_std(df[["A"]], "A")
        pd.testing.assert_frame_equal(df_result, df_expected.iloc[[0], :].reset_index(drop=True))

        df_result = null_suspicious_dob_std(df[["B"]], "B")
        pd.testing.assert_frame_equal(df_result, df_expected.iloc[[1], :].reset_index(drop=True))

        df_result = null_suspicious_dob_std(df[["C"]], "C")
        pd.testing.assert_frame_equal(df_result, df_expected.iloc[[2], :].reset_index(drop=True))

    def test_fix_string(self):
        names_list = [
            {"id": 1, "first_name": "", "surname": "a"},
            {"id": 2, "first_name": " ", "surname": "b"},
            {"id": 3, "first_name": " john", "surname": None},
        ]

        df = pd.DataFrame(names_list)
        df = df[names_list[0].keys()]

        df_result = fix_zero_length_strings(df)

        df_expected = [
            {"id": 1, "first_name": None, "surname": "a"},
            {"id": 2, "first_name": None, "surname": "b"},
            {"id": 3, "first_name": "john", "surname": None},
        ]

        df_expected = pd.DataFrame(df_expected)

        pd.testing.assert_frame_equal(df_result, df_expected)

    def test_names(self):
        names_list = [
            {"id": 1, "first_name": "John-paul", "surname": "smith jones"},
            {"id": 2, "first_name": "john", "surname": "Smith-Jones"},
            {"id": 3, "first_name": "john.smith", "surname": "jones"},
        ]

        df = pd.DataFrame(names_list)
        df_result = standardise_names(df, ["first_name", "surname"])

        df_expected = [
            {
                "id": 1,
                "surname_std": "jones",
                "forename1_std": "john",
                "forename2_std": "paul",
                "forename3_std": "smith",
                "forename4_std": None,
                "forename5_std": None,
            },
            {
                "id": 2,
                "surname_std": "jones",
                "forename1_std": "john",
                "forename2_std": "smith",
                "forename3_std": None,
                "forename4_std": None,
                "forename5_std": None,
            },
            {
                "id": 3,
                "surname_std": "jones",
                "forename1_std": "john",
                "forename2_std": "smith",
                "forename3_std": None,
                "forename4_std": None,
                "forename5_std": None,
            },
        ]

        df_expected = pd.DataFrame(df_expected)

        pd.testing.assert_frame_equal(df_result, df_expected)

        # This test tests standarisation options we do not use as standard

    def test_pnc_1(self):
        pnc_list = [
            {"id": 1, "pnc_number": "2012/0443841T"},
            {"id": 2, "pnc_number": " 2012/0443841T "},
            {"id": 3, "pnc_number": "2012/0443841t"},
            {"id": 4, "pnc_number": "2012/ 0443841t"},
            {"id": 5, "pnc_number": "2012/0443841TA"},
            {"id": 6, "pnc_number": None},
        ]

        df = pd.DataFrame(pnc_list)
        df_result = standardise_pnc_number(df, "pnc_number")

        df_expected = [
            {"id": 1, "pnc_number_std": "2012/0443841T"},
            {"id": 2, "pnc_number_std": "2012/0443841T"},
            {"id": 3, "pnc_number_std": "2012/0443841T"},
            {"id": 4, "pnc_number_std": "2012/0443841T"},
            {"id": 5, "pnc_number_std": None},
            {"id": 6, "pnc_number_std": None},
        ]

        df_expected = pd.DataFrame(df_expected)

        pd.testing.assert_frame_equal(df_result, df_expected)

    def test_pnc_2(self):
        pnc_list = [
            {"id": 1, "pnc_number": "2012/0443841T"},
            {"id": 2, "pnc_number": " 2012/0443841T "},
            {"id": 3, "pnc_number": "2012/0443841t"},
            {"id": 4, "pnc_number": "2012/ 0443841t"},
            {"id": 5, "pnc_number": "2012/0443841TA"},
            {"id": 6, "pnc_number": None},
        ]

        df = pd.DataFrame(pnc_list)
        df_result = standardise_pnc_number(df, "pnc_number", drop_orig=False)

        df_expected = [
            {"id": 1, "pnc_number": "2012/0443841T", "pnc_number_std": "2012/0443841T"},
            {"id": 2, "pnc_number": " 2012/0443841T ", "pnc_number_std": "2012/0443841T"},
            {"id": 3, "pnc_number": "2012/0443841t", "pnc_number_std": "2012/0443841T"},
            {"id": 4, "pnc_number": "2012/ 0443841t", "pnc_number_std": "2012/0443841T"},
            {"id": 5, "pnc_number": "2012/0443841TA", "pnc_number_std": None},
            {"id": 6, "pnc_number": None, "pnc_number_std": None},
        ]

        df_expected = pd.DataFrame(df_expected)

        pd.testing.assert_frame_equal(df_result, df_expected)

    def test_pnc_3(self):
        pnc_list = [
            {"id": 1, "pnc_number": "1933/0993399H"},
            {"id": 2, "pnc_number": "2033/0993399H"},
            {"id": 3, "pnc_number": "2003/0993399B"},
            {"id": 4, "pnc_number": "1999/0339933H"},
            {"id": 5, "pnc_number": "2003/0993399H"},
        ]

        df = pd.DataFrame(pnc_list)
        df_result = standardise_pnc_number(df, "pnc_number", drop_orig=False)

        df_expected = [
            {"id": 1, "pnc_number": "1933/0993399H", "pnc_number_std": None},
            {"id": 2, "pnc_number": "2033/0993399H", "pnc_number_std": None},
            {"id": 3, "pnc_number": "2003/0993399B", "pnc_number_std": None},
            {"id": 4, "pnc_number": "1999/0339933H", "pnc_number_std": None},
            {"id": 5, "pnc_number": "2003/0993399H", "pnc_number_std": None},
        ]

        df_expected = pd.DataFrame(df_expected)

        pd.testing.assert_frame_equal(df_result, df_expected)
