# -*- coding: utf-8 -*-
import pandas as pd


def drop_na(df, columns):
    df.dropna(subset=columns, inplace=True)


def parse_number_or_drop(df, columns):
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    drop_na(df, columns)

    for col in columns:
        df[col] = df[col].astype(int)


def parse_string(df, columns):
    for col in columns:
        df[col] = df[col].astype(str)


def drop_duplicates(df, columns):
    df.drop_duplicates(subset=columns, inplace=True)


def fill_na(df, columns, value):
    for col in columns:
        df[col] = df[col].fillna(value)
