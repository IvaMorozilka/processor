import pandas as pd
import re


def multi_replace(df: pd.DataFrame, column: str, replace_regex_dict: str):
    def helper(text, replacements):
        pattern = re.compile("|".join(re.escape(k) for k in replacements.keys()))
        return pattern.sub(lambda m: replacements[m.group(0)], text)

    df[column] = df[column].apply(lambda x: helper(x, replace_regex_dict))


def uppercase_first_letter(df: pd.DataFrame, column: str):
    df[column] = df[column].apply(lambda x: x[0].upper() + x[1:])


def prepare(df: pd.DataFrame):
    # Преобразуем обжект в стринги
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype("string")

    # Удаляем всякое г до и после строки
    df = df.map(lambda v: re.sub(r"^\s+|\s+$", "", v) if isinstance(v, str) else v)
    # Удаляем переносы везде (зачем они?)
    df = df.map(lambda v: v.strip().replace("\n", " ") if isinstance(v, str) else v)

    df.rename(
        mapper=lambda v: re.sub(r"^[^a-zA-Zа-яА-Я№]+", "", v),
        axis="columns",
        inplace=True,
    )


def classification(
    df: pd.DataFrame, replace_dict: dict, column_from: str, column_to: str
):
    def replace_val(value):
        for replace_string, replace_values in replace_dict.items():
            for val in replace_values:
                if val.lower() in value.lower():
                    return replace_string
        return value

    df[column_to] = df[column_from].apply(replace_val)

    not_replaced_values = df[df[column_to].isin(replace_dict.keys()) == False][  # noqa: E712
        column_from
    ].unique()
    return not_replaced_values


def null_replacement(df: pd.DataFrame, replace_dict: dict):
    for column, replace_with in replace_dict.items():
        df[column] = df[column].fillna(value=replace_with)


def type_conversion(df: pd.DataFrame, conversion_dict: dict):
    for column, convert_to in conversion_dict.items():
        df[column] = df[column].astype(convert_to)
