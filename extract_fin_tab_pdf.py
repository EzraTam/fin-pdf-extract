"""Module for extracting data from pdf table
"""

# Imports
from re import sub
from typing import Dict
from tabula.io import read_pdf
import pandas as pd
import numpy as np


def replace_special_char(input_string: str, char_replacement=" ") -> str:
    """Replace special characters from an input string.

    Args:
        input_string (str): String to be procesed
        char_replacement (str, optional): Character to replace the special characters.
        Defaults to " ".

    Returns:
        str: Result string
    """
    return sub(r"[^a-zA-Z0-9 \n\.]", char_replacement, input_string)


def join_string_from_list(input_list: list, char_join=" ") -> str:
    """Give a list of strings. Join them with a given separator.

    Args:
        input_list (list): String to be processed
        char_join (str, optional): Character used for joining the list of input strings.
        Defaults to " ".

    Returns:
        str: Result string
    """
    return char_join.join(input_list)


def snake_case(input_string: str) -> str:
    """Transform input string to snake case

    Args:
        input_string (str): String to be processed

    Returns:
        str: Result string
    """
    return "_".join(
        sub(
            "([A-Z][a-z]+)",
            r" \1",
            sub("([A-Z]+)", r" \1", input_string.replace("-", " ")),
        ).split()
    ).lower()


def delete_fin_unit(string: str) -> Dict[str, str]:
    """Separate the financial unit from a string.
    Financial units are dollar and percent.
    Example: "$ 180" -> {"val": "180", "unit": "$"}

    Args:
        string (str): String possibly containing financial unit.

    Returns:
        dict: val -> value
              unit -> unit
    """

    li_spec_char = ["$", "%"]

    for char in li_spec_char:

        if string.find(char) != -1:

            string_split = string.split(" ")
            ind_spec_char = string_split.index(char)
            string_split.pop(ind_spec_char)
            return dict(val=string_split[0], unit=char)

        else:
            return dict(val=string, unit="nan")


def get_val_unit(dataframe: pd.DataFrame, col_nm: str) -> pd.DataFrame:
    """Get value and unit of a column as df

    Args:
        dataframe (pd.DataFrame): Input DF
        col_nm (str): Column name where the value and unit
        should be extracted from

    Returns:
        pd.DataFrame: DF with value and unit of the chosen column
    """
    return (
        dataframe[col_nm]
        .astype(str)
        .apply(delete_fin_unit)
        .apply(pd.Series)
        .astype(object)
        .replace("nan", np.nan)
    )


def check_unit(df_val_unit: pd.DataFrame) -> Dict[bool, str]:
    """Check whether the output of get_val_unit indeed contains unit

    Args:
        df_val_unit (pd.DataFrame): DF output of get_val_unit

    Returns:
        Dict[bool,str]: dict with flag whether unit exists or not
        and corresponding unit.
    """
    temp_res = df_val_unit["unit"].agg(lambda x: list(set(x.dropna())))
    if len(temp_res) == 1:
        return dict(fg_unit=True, unit=temp_res[0])
    else:
        return dict(fg_unit=False, unit=None)


def find_unit_cols(dataframe: pd.DataFrame) -> pd.Series:
    """Find columns containing financial units

    Args:
        dataframe (pd.DataFrame): Input DF

    Returns:
        pd.Series: Column name and the corresponding units
    """
    li_el_in_cols = dataframe.agg(lambda x: list(set(x.dropna())))
    count_el = li_el_in_cols.apply(len)
    col_singleton = count_el[count_el == 1]
    col_singleton = list(col_singleton.keys())
    return li_el_in_cols[col_singleton]


class FinTabPdf:
    """Class for handling financial table
    from PDF
    """

    def __init__(self, path_pdf: str, page: int):
        """Initialization

        Args:
            path_pdf (str): Path of the PDF where the table should be extracted
            page (int): Page where the table of interest is given
        """

        # Load the pdf table to pandas format
        self.df_pdf_raw = read_pdf(path_pdf, pages=page)[0].rename(
            columns={"Unnamed: 0": "row_nm"}
        )

        self._col_nm_extracted = pd.Series(dtype="object")

        self.df_preproc = pd.DataFrame()

        self.df_nan_row_nm = pd.DataFrame()

        self.title_rows = pd.Series(dtype="object")

        self.units = dict()

    @staticmethod
    def _sep_nan_row_nm(df_raw: pd.DataFrame) -> Dict[pd.DataFrame, pd.DataFrame]:
        """Position/row names usually in row_nm.
        Extract those rows which has nan entry in that column

        Args:
            df_raw (pd.DataFrame): _description_

        Returns:
            Dict[pd.DataFrame, pd.DataFrame]: _description_
        """

        df_nan_row_nm = df_raw[pd.isna(df_raw["row_nm"])]
        df_res = df_raw.drop(df_nan_row_nm.index)

        return dict(nan_row_nm=df_nan_row_nm, res=df_res)

    @staticmethod
    def _clean_row_nm(df_input: pd.DataFrame) -> pd.DataFrame:
        """Clean the column row name.
           Delete footnote citation
           make snake_case notation
        Args:
            df_input (pd.DataFrame): input df having row_nm column

        Returns:
            pd.DataFrame: df with row_nm col cleaned
        """

        df_input["row_nm"] = df_input["row_nm"].apply(lambda x: x.split("(")[0])
        df_input["row_nm"] = df_input["row_nm"].apply(replace_special_char)
        df_input["row_nm"] = df_input["row_nm"].apply(snake_case)

        return df_input

    @staticmethod
    def _delete_title_row(df_input: pd.DataFrame) -> Dict[pd.DataFrame, pd.Series]:
        """Delete title row having only nan-values"""
        df_temp = df_input.drop(columns=["row_nm"])
        df_temp = df_temp.dropna(how="all")

        return dict(
            cleaned_title_row=df_input.drop(
                list(set(df_input.index) - set(df_temp.index))
            ),
            title_rows=df_input.drop(df_temp.index)["row_nm"],
        )

    @staticmethod
    def _extract_unit_cols(df_input: pd.DataFrame) -> Dict[pd.DataFrame, Dict]:

        # Remove unit columns
        li_col = list(df_input.columns)
        ser_unit_cols = find_unit_cols(df_input)

        for col in ser_unit_cols.keys():
            li_col.remove(col)

        # Extract unit from columns
        for col in li_col:
            # get value and unit of the column
            col_val_unit = get_val_unit(dataframe=df_input, col_nm=col)

            # Add unit column whether we have unit in the unit column
            dict_check_unit = check_unit(col_val_unit)

            if dict_check_unit["fg_unit"]:

                # get the index of the column
                idx_col = df_input.columns.get_loc(col)

                if dict_check_unit["unit"] == "%":
                    idx_col = idx_col + 1

                df_input[col] = col_val_unit["val"]
                df_input.insert(
                    loc=idx_col, column=f"new_col: {col}", value=col_val_unit["unit"]
                )

        # Extract all unit cols
        ser_unit_cols = find_unit_cols(df_input)

        dict_unit = {}

        for col in ser_unit_cols.keys():
            idx = list(df_input.columns).index(col)
            if ser_unit_cols[col][0] == "$":
                dict_unit[df_input.columns[idx + 1]] = "$"
            elif ser_unit_cols[col][0] == "%":
                dict_unit[df_input.columns[idx - 1]] = "%"

        df_input = df_input.drop(columns=list(ser_unit_cols.keys()))

        return dict(result=df_input, units=dict_unit)

    def preprocess_raw(self) -> pd.DataFrame:
        """Method to preprocess data

        Returns:
            pd.DataFrame: Preprocessed data
        """

        # Separate rows with nan row_nm column entries
        nan_row_sep = self._sep_nan_row_nm(self.df_pdf_raw)
        self.df_nan_row_nm = nan_row_sep["nan_row_nm"]
        self.df_preproc = nan_row_sep["res"]

        # Clean row_nm column entries
        self.df_preproc = self._clean_row_nm(self.df_preproc)

        # Clean title rows
        cleaned_title = self._delete_title_row(self.df_preproc)
        self.title_rows = cleaned_title["title_rows"]
        self.df_preproc = cleaned_title["cleaned_title_row"]

        # Extract Units
        extract_unit = self._extract_unit_cols(self.df_preproc)
        self.df_preproc = extract_unit["result"]
        self.units = extract_unit["units"]

        return self.df_preproc

    def extract_col_nm(self, li_col_idx: list) -> pd.Series:
        """Function for extracting column names as the might be given in certain columns

        Args:
            li_col_idx (list, optional): List of column indexes, where the column names are given.
            Defaults to [0,1].

        Returns:
            pd.Series: Series with column names and the real column names
        """
        self._col_nm_extracted = (
            self.df_pdf_raw.iloc[li_col_idx]
            .agg(lambda x: list(x.dropna()))
            .apply(lambda x: snake_case(replace_special_char(join_string_from_list(x))))
        )
        self._col_nm_extracted.replace(" ", np.nan, inplace=True)
        self._col_nm_extracted = self._col_nm_extracted.dropna()
        return self._col_nm_extracted
