from re import sub
import pandas as pd
import numpy as np

def replace_special_char(input_string:str,char_replacement=" "):
    return sub(r'[^a-zA-Z0-9 \n\.]', char_replacement, input_string)
def join_string_from_list(input_list:list,char_join=" "):
    return char_join.join(input_list)
def snake_case(input_string:str)->str:
    return '_'.join(
        sub('([A-Z][a-z]+)', r' \1',
        sub('([A-Z]+)', r' \1',
        input_string.replace('-', ' '))).split()).lower()

def extract_col_nm(dataframe:pd.DataFrame):
    return dataframe.iloc[[0,1]].agg(lambda x: list(x.dropna())).apply(lambda x: snake_case(replace_special_char(join_string_from_list(x))))

def find_unit_cols(dataframe:pd.DataFrame):
    li_el_in_cols=dataframe.agg(lambda x: list(set(x.dropna())))
    count_el=li_el_in_cols.apply(len)
    col_singleton=count_el[count_el==1]
    col_singleton=list(col_singleton.keys())
    return li_el_in_cols[col_singleton]

def delete_fin_unit(string:str)->dict:
    li_spec_char=["$","%"]

    for char in li_spec_char:
        
        if string.find(char)!=-1:
            
            string_split=string.split(" ")
            ind_spec_char=string_split.index(char)
            string_split.pop(ind_spec_char)
            return dict(
                val=string_split[0],
                unit=char
                )
        else:
            return dict(
                val=string,
                unit="nan"
                )

def get_val_unit(dataframe:pd.DataFrame,col_nm:str)->pd.DataFrame:
    return dataframe[col_nm].astype(str).apply(delete_fin_unit).apply(pd.Series).astype(object).replace("nan",np.nan)

def check_unit(df_val_unit:pd.DataFrame):
    temp_res=df_val_unit["unit"].agg(lambda x: list(set(x.dropna())))
    if len(temp_res) == 1:
        return dict(
            fg_unit=True,
            unit=temp_res[0]
            )
    else:
        return dict(
            fg_unit=False,
            unit=None
            )
    

def extract_year(dataframe:pd.DataFrame):

    for col in dataframe.columns:
        if col.isdigit():
            return col
    
    # If no year column
    return None

def separate_df(dataframe:pd.DataFrame,num_cols_group:int):
    num_cols=len(dataframe.columns)
    li_dict_df=[]

    for idx in range(0,num_cols,num_cols_group):
        df_separated=dataframe[dataframe.columns[idx:idx+num_cols_group]]
        li_dict_df.append(
            dict(
                year=extract_year(df_separated),
                df=df_separated
            )
        )

    return li_dict_df

def assign_col_nms_from_desc(dataframe:pd.DataFrame,dict_col_trans)->dict:

    dict_col_nm_map={}
    for col_nm in dataframe:
        if col_nm in dict_col_trans.keys():
            dict_col_nm_map[col_nm]=dict_col_trans[col_nm]

    return dict_col_nm_map