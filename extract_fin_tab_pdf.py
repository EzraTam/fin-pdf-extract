"""Module for extracting data from pdf table
"""

# Imports
import tabula
import pandas as pd
from re import sub
import numpy as np


def replace_special_char(input_string: str, char_replacement=" ") -> str:
    """Replace special characters from an input string.

    Args:
        input_string (str): String to be procesed
        char_replacement (str, optional): Character to replace the special characters. Defaults to " ".

    Returns:
        str: Result string
    """
    return sub("[^a-zA-Z0-9 \n\.]", char_replacement, input_string)


def join_string_from_list(input_list: list, char_join=" ") -> str:
    """Give a list of strings. Join them with a given separator.

    Args:
        input_list (list): String to be processed
        char_join (str, optional): Character used for joining the list of input strings. Defaults to " ".

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
        self.df_pdf_raw = tabula.read_pdf(path_pdf, pages=page)[0]

        self._col_nm_extracted = pd.Series(dtype="object")

    def extract_col_nm(self, li_col_idx=[0, 1]) -> pd.Series:
        """Function for extracting column names as the might be given in certain columns

        Args:
            li_col_idx (list, optional): List of column indexes, where the column names are given. Defaults to [0,1].

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
