# -*- coding: utf-8 -*-
from __future__ import annotations

import csv
import os
import re
import stat
from builtins import Ellipsis
from collections.abc import Iterable

import chardet
import numpy
import pandas

import kb_tools.tools as tools


class DatasetFactory:
    is_null = pandas.isnull
    NAN = numpy.nan

    def __init__(
        self, dataset: str | pandas.DataFrame | list | dict = None, **kwargs
    ):
        self.__path = None
        if isinstance(dataset, str):
            self.__path = dataset
        if dataset is None:
            self.__source = pandas.DataFrame()
        elif not isinstance(dataset, pandas.DataFrame) or (
            hasattr(dataset, "readable") and dataset.readable()
        ):
            self.__source = self.from_file(dataset, **kwargs).dataset
        else:
            columns = self._parse_columns_arg(
                kwargs.get("columns"), dataset.columns
            )

            if columns is not None:
                dataset.columns = dataset.columns.astype(str)
                # print(columns)
                # dataset = dataset.loc[:, columns.keys()]
                dataset.rename(columns=columns, inplace=True)
                dataset = dataset.loc[:, columns.values()]
            self.__source = dataset

        self.columns = pandas.Index(
            [tools.Var(col, force=True) for col in self.__source.columns]
        )

    # Ok
    @staticmethod
    def __parse_col(col, columns):
        for item in columns:
            if tools.Var(item) == col:
                return item
        for item in columns:
            if tools.Var(item, force=True) == col:
                return item
        return col

    # Ok

    @staticmethod
    def _parse_columns_arg(columns, dataset_columns):
        if isinstance(columns, Iterable):
            final_col = {}
            dataset_columns = [
                tools.Var(col, force=True) for col in dataset_columns
            ]
            first = next(iter(columns))
            without = False
            if first is Ellipsis:
                without = True
                final_col = {k: k for k in dataset_columns}

            for k in columns:
                got_k = k
                alias = None
                if k is Ellipsis:
                    continue
                if isinstance(k, dict):
                    k, alias = next(iter(k.items()))
                if k in dataset_columns:
                    key = DatasetFactory.__parse_col(k, dataset_columns)
                    alias = (
                        columns[k] if isinstance(columns, dict) else alias or k
                    )
                elif isinstance(k, str):
                    # à
                    key = k
                    alias = (
                        columns[k] if isinstance(columns, dict) else alias or k
                    )
                elif isinstance(k, int):
                    key = dataset_columns[k]
                    alias = (
                        columns[k]
                        if isinstance(columns, dict)
                        else alias or key
                    )
                else:
                    raise ValueError("Bad column %s given" % k)
                if without and not isinstance(got_k, dict):
                    final_col.pop(key)
                else:
                    final_col[key] = alias
            return final_col

    @staticmethod
    def _check_delimiter(sample, check_in=None):
        if check_in is None:
            check_in = [",", "\t", ";", " ", ":", "$", "|"]
        first_lines = "".join(sample)
        try:
            sep = (
                csv.Sniffer()
                .sniff(first_lines[:-1], delimiters=check_in)
                .delimiter
            )
        except csv.Error:
            sep = None
        return sep

    @classmethod
    def from_file(
        cls, file_path, sep=None, columns=None, force_encoding=True, **kwargs
    ):

        delimiters = kwargs.pop("delimiters", None)
        if "header" in kwargs and isinstance(kwargs["header"], bool):
            kwargs["header"] = None if not kwargs["header"] else "infer"
        if isinstance(file_path, cls):
            dataset = file_path.dataset
        elif isinstance(file_path, str):
            try:
                is_hidden = bool(
                    os.stat(file_path).st_file_attributes
                    & stat.FILE_ATTRIBUTE_HIDDEN
                )
            except AttributeError:
                # on Linux
                is_hidden = False
            if is_hidden:
                dataset = pandas.DataFrame()
            elif os.path.splitext(file_path)[1][1:].lower() in [
                "xls",
                "xlsx",
                "xlsm",
                "xlsb",
            ]:
                kwargs_ = {
                    k: v
                    for k, v in kwargs.items()
                    if k in tools.get_func_args(pandas.read_excel)
                }
                dataset = pandas.read_excel(file_path, **kwargs_)
            else:
                kwargs_ = {
                    k: v
                    for k, v in kwargs.items()
                    if k in tools.get_func_args(pandas.read_csv)
                }
                if "encoding" not in kwargs:
                    kwargs_["encoding"] = "utf-8"
                else:
                    kwargs_["encoding"] = kwargs["encoding"]
                used_sniffer = False
                try:
                    if sep is None:
                        with open(
                            file_path, encoding=kwargs_["encoding"]
                        ) as file:
                            sample = [file.readline() for _ in range(10)]
                            try:
                                sep = cls._check_delimiter(sample, delimiters)
                                assert sep, ""
                                kwargs_["sep"] = sep
                                used_sniffer = True
                            except (csv.Error, AssertionError):
                                pass
                    else:
                        kwargs_["sep"] = sep
                    dataset = pandas.read_csv(file_path, **kwargs_)
                except UnicodeDecodeError as exc:
                    if not force_encoding:
                        raise exc

                    last_min_buffer = None
                    while True:
                        min_buffer = (
                            int(
                                re.search(
                                    r"position (\d+):", str(exc)
                                ).groups()[0]
                            )
                            + 100
                        )
                        if min_buffer == last_min_buffer:
                            raise exc
                        with open(file_path, "rb") as file_from_file_path:
                            file_bytes = file_from_file_path.read(min_buffer)
                            encoding_proba = chardet.detect(file_bytes).get(
                                "encoding", "latin1"
                            )
                            if str(encoding_proba).lower() == "ascii":
                                encoding_proba = "cp1252"
                        if kwargs_["encoding"] != encoding_proba:
                            kwargs_["encoding"] = encoding_proba
                            try:
                                if "sep" not in kwargs_ or used_sniffer:
                                    with open(
                                        file_path, encoding=kwargs_["encoding"]
                                    ) as file:
                                        sample = [
                                            file.readline() for _ in range(10)
                                        ]
                                        try:
                                            sep = cls._check_delimiter(
                                                sample, delimiters
                                            )
                                            assert sep, ""
                                            kwargs_["sep"] = sep
                                        except (csv.Error, AssertionError):
                                            pass
                                dataset = pandas.read_csv(file_path, **kwargs_)
                                break
                            except UnicodeDecodeError as ex:
                                kwargs_["encoding"] = None
                                exc = ex
                        else:
                            raise exc
                        last_min_buffer = min_buffer
        elif hasattr(file_path, "readable") and file_path.readable():
            sample = [file_path.readline() for _ in range(10)]
            file_path.seek(0)
            kk = {}
            if sep is None:
                try:
                    sep = DatasetFactory._check_delimiter(sample, delimiters)
                    assert sep, ""
                    kk["delimiter"] = sep
                except (csv.Error, AssertionError):
                    pass
            else:
                kk["delimiter"] = sep
            dataset = pandas.DataFrame(csv.DictReader(file_path, **kk))
        else:
            col_arg = columns
            if isinstance(columns, list):
                columns = None

            dataset = pandas.DataFrame(
                file_path,
                columns=col_arg,
                **{k: v for k, v in kwargs.items() if k in ["index", "dtype"]},
            )

        return cls(dataset, columns=columns)

        # Ok

    def save(self, path=None, force=False, chdir=True, **kwargs):
        path = path or self.__path

        if path is None:
            raise TypeError("save method required :param path argument")
        if chdir:
            self.__path = path
        if "index" not in kwargs:
            kwargs["index"] = False
        _base, ext = os.path.splitext(path)
        if force:
            i = 1
            while os.path.exists(path):
                path = _base + "_" + str(i) + ext
                i += 1
        if ext.lower() in [".xls", ".xlsx", ".xlsb"]:
            self.__source.to_excel(path, **kwargs)
        elif ext.lower() in [".csv", ".txt", ""]:
            self.__source.to_csv(path, **kwargs)
        return path

    # Ok
    @property
    def dataset(self):
        return self.__source.rename(
            columns={
                k: str(self.columns[i])
                for i, k in enumerate(self.__source.columns)
            }
        )


if __name__ == "__main__":
    pass
