import pandas as pd
import re
from datetime import datetime


def multi_replace(
    df: pd.DataFrame, column: str, replace_regex_dict: dict
) -> pd.DataFrame:
    def helper(text, replacements):
        pattern = re.compile("|".join(re.escape(k) for k in replacements.keys()))
        return pattern.sub(lambda m: replacements[m.group(0)], text)

    df_copy = df.copy()
    df_copy[column] = df_copy[column].apply(lambda x: helper(x, replace_regex_dict))
    return df_copy


def uppercase_first_letter(df: pd.DataFrame, column: str) -> pd.DataFrame:
    df_copy = df.copy()
    df_copy[column] = df_copy[column].apply(
        lambda x: x[0].upper() + x[1:] if isinstance(x, str) and len(x) > 0 else x
    )
    return df_copy


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    # Преобразуем обжект в стринги
    for col in df_copy.columns:
        if df_copy[col].dtype == "object":
            df_copy[col] = df_copy[col].astype("string")

    # Удаляем всякое г до и после строки
    df_copy = df_copy.map(
        lambda v: re.sub(r"^\s+|\s+$", "", v) if isinstance(v, str) else v
    )
    # Удаляем переносы везде (зачем они?)
    df_copy = df_copy.map(
        lambda v: v.strip().replace("\n", " ") if isinstance(v, str) else v
    )

    df_copy = df_copy.rename(
        mapper=lambda v: re.sub(r"^[^a-zA-Zа-яА-Я№]+", "", v),
        axis="columns",
    )
    return df_copy


def classification(
    df: pd.DataFrame, replace_dict: dict, column_from: str, column_to: str
) -> pd.DataFrame:
    def replace_val(value):
        for replace_string, replace_values in replace_dict.items():
            for val in replace_values:
                if val.lower() in value.lower():
                    return replace_string
        return value

    df_copy = df.copy()
    df_copy[column_to] = df_copy[column_from].apply(replace_val)

    #   not_replaced_values = df_copy[df_copy[column_to].isin(replace_dict.keys()) == False][  # noqa: E712
    #       column_from
    #   ].unique()
    return df_copy


def null_replacement(df: pd.DataFrame, replace_dict: dict) -> pd.DataFrame:
    df_copy = df.copy()
    for column, replace_with in replace_dict.items():
        df_copy[column] = df_copy[column].fillna(value=replace_with)
    return df_copy


def type_conversion(df: pd.DataFrame, conversion_dict: dict) -> pd.DataFrame:
    df_copy = df.copy()
    for column, convert_to in conversion_dict.items():
        df_copy[column] = df_copy[column].astype(convert_to)
    return df_copy


def set_month_names(
    df: pd.DataFrame, month_column_int: str, month_column_str: str
) -> pd.DataFrame:
    month_names = {
        1: "Январь",
        2: "Февраль",
        3: "Март",
        4: "Апрель",
        5: "Май",
        6: "Июнь",
        7: "Июль",
        8: "Август",
        9: "Сентябрь",
        10: "Октябрь",
        11: "Ноябрь",
        12: "Декабрь",
    }
    df_copy = df.copy()
    df_copy[month_column_str] = df_copy[month_column_int].astype(int).map(month_names)
    return df_copy


def extract_month_and_year(file_name: str):
    month_names = {
        1: "Январь",
        2: "Февраль",
        3: "Март",
        4: "Апрель",
        5: "Май",
        6: "Июнь",
        7: "Июль",
        8: "Август",
        9: "Сентябрь",
        10: "Октябрь",
        11: "Ноябрь",
        12: "Декабрь",
    }
    str_date = file_name.split("_")[1]
    date = datetime.strptime(str_date, "%d-%m-%Y")
    month, year = date.month, date.year
    return month_names[month], year
