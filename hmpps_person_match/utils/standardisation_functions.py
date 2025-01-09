import numpy as np
import pandas as pd

bad_pnc_list = (
    "1933/0993399H",
    "2033/0993399H",
    "2003/0993399B",
    "1999/0339933H",
    "2003/0993399H",
)


def standardise_pnc_number(df, pnc_col="pnc_number", drop_orig=True):
    """Create a standardised column called pnc_number_std
    Args:
        df (DataFrame): Pandas dataframe
        pnc_col (str): Name of PNC number column
        drop_orig (bool, optional): Drop original PNC number column. Defaults to True.
    Returns:ruff
        DataFrame: Pandas DataFrame with new standardised PNC number column called pnc_number_std
    """
    data = df.copy(deep=True)

    # Special case where dob is passed as "pnc": {} so is filled with np.nan i.e. float,
    # here we fill with None and cast to string
    if data[pnc_col].dtype == "float64":
        data = data.replace({np.nan: None})
        data["pnc_number_std"] = df[pnc_col].astype("str")
    else:
        data["pnc_number_std"] = data[pnc_col].str.replace(" ", "").str.upper()
        data["pnc_number_std"] = data["pnc_number_std"].apply(lambda x: x if len(str(x)) == 13 else None)

        data["pnc_number_std"] = data["pnc_number_std"].apply(lambda x: x if x not in bad_pnc_list else None)

    if drop_orig and pnc_col != "pnc_number_std":
        data = data.drop(pnc_col, axis=1)

    data = data.replace({"nan": None})

    return data


def standardise_dob(
    df,
    dob_col,
    drop_orig=True,
):
    """Create column called dob_std with dob as a string in yyyy-MM-dd format
    or null otherwise
    Args:
        df : Pandas dataframe
        dob_col (str): Name of dob column
        drop_orig (bool, optional): Drop original date of birth column. Defaults to True.
    Returns:
        DataFrame: Pandas DataFrame with new standardised dob column called dob_std
    """

    df = df.replace({np.nan: None})

    dtypes = dict(df.dtypes)

    if dtypes[dob_col] not in ("O", "datetime64[ns]", "float64"):
        raise TypeError("DoB column type needs to be string or datetime")
    else:
        if dtypes[dob_col] == "datetime64[ns]":
            df["dob_std"] = df[dob_col].dt.strftime("%Y-%m-%d")

        if dtypes[dob_col] == "O":
            df["dob_std"] = pd.to_datetime(df[dob_col], yearfirst=True).dt.strftime("%Y-%m-%d")

        # Special case where dob is passed as "dob": {} so is filled with np.nan
        # i.e. float, here we fill with None and cast to datetime
        if dtypes[dob_col] == "float64":
            df = df.replace({np.nan: None})
            df["dob_std"] = df[dob_col].astype("datetime64[ns]")

        if drop_orig and dob_col != "dob_std":
            df = df.drop(dob_col, axis=1)
    return df


def standardise_names(df, name_cols, drop_orig=True):
    """Take a one or more name columns in a list and standardise the names
    so one name appears in each column consistently
    Args:
        df (DataFrame): Pandas DataFrame
        name_cols (list): A list of columns that contain names, in order from first name to last name
        drop_orig (bool, optional): Drop the original columns after standardisation. Defaults to True.
    Returns:
        DataFrame: A pandas DataFrame with standardised name columns
    """

    if all(df[name_cols].dtypes == "float64"):
        df[name_cols] = df[name_cols].replace({np.nan: None})
        df[name_cols] = df[name_cols].astype("str")
        df["surname_std"] = None
        df["forename1_std"] = None
        df["forename2_std"] = None
        df["forename3_std"] = None
        df["forename4_std"] = None
        df["forename5_std"] = None

    else:
        surname_col_name = name_cols[-1]

        df["name_concat"] = df[name_cols].apply(lambda row: " ".join(row.values.astype(str)), axis=1).str.lower()
        df["name_concat"] = df["name_concat"].str.replace("-", " ").str.replace(".", " ")
        df["name_arr"] = df["name_concat"].str.split(" ")
        df["surname_std"] = (
            df["name_arr"].apply(lambda x: x[-1] if df[surname_col_name] is not None else None).replace("nan", np.nan)
        )
        df["forename1_std"] = (
            df["name_arr"].apply(lambda x: x[0] if len(x) > 1 else None).replace("nan", None).replace("nan", np.nan)
        )
        df["forename2_std"] = (
            df["name_arr"].apply(lambda x: x[1] if len(x) > 2 else None).replace("nan", None).replace("nan", np.nan)
        )
        df["forename3_std"] = (
            df["name_arr"].apply(lambda x: x[2] if len(x) > 3 else None).replace("nan", None).replace("nan", np.nan)
        )
        df["forename4_std"] = (
            df["name_arr"].apply(lambda x: x[3] if len(x) > 4 else None).replace("nan", None).replace("nan", np.nan)
        )
        df["forename5_std"] = (
            df["name_arr"].apply(lambda x: x[4] if len(x) > 5 else None).replace("nan", None).replace("nan", np.nan)
        )
        df.drop(["name_concat", "name_arr"], axis=1, inplace=True)

    if drop_orig:
        for n in name_cols:
            df = df.drop(n, axis=1)

    df = df.replace({np.nan: None})
    df = df.replace({"none": None})

    return df


def fix_zero_length_strings(df):
    """Convert any zero length strings or strings that contain only whitespace to a true null
    Args:
        df (DataFrame): Input Pandas dataframe
    Returns:
        DataFrame: Pandas Dataframe with clean strings
    """
    string_cols = df.select_dtypes(include="object").columns.tolist()
    df[string_cols] = df[string_cols].map(
        lambda x: None if (x is None or x == np.nan or len(x) == 0 or set(x) == set(" ")) else x.strip(),
    )
    return df


def null_suspicious_dob_std(df, dob_col="dob_std"):
    """Null out suspicious dates of birth
    Args:
        df (DataFrame): Input Spark DataFrame.  Expects that dob column has already been standardised
        dob_col (str, optional): Name of standardised dob col. Defaults to "dob_std".
    Returns:
        DataFrame: Original dataframe with suspicious dates of birth nulled out
    """
    df[dob_col] = df[dob_col].apply(lambda x: x if x not in ["1900-01-01", "1970-01-01"] else None)

    df["dob_std"] = df[dob_col]
    if dob_col != "dob_std":
        df = df.drop(dob_col, axis=1)

    return df
