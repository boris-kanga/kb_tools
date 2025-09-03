# -*- coding: utf-8 -*-
from __future__ import annotations

import abc
import csv
import decimal
import os

import pandas

try:
    from tqdm import tqdm
except ImportError:

    def tqdm(iter_obj, total=None):
        return (x for x in iter_obj)


from kb_tools.tools import INFINITE, Cdict, get_buffer, get_no_filepath
from kb_tools.utils.fdataset import DatasetFactory


class BaseDB(abc.ABC):
    DEFAULT_PORT = None
    MAX_BUFFER_INSERTING_SIZE = 2000
    LAST_REQUEST_COLUMNS = None

    def __init__(self, **kwargs):
        self._kwargs = kwargs

        self._print_info = print
        self._print_error = print
        self.set_logger(self._kwargs.get("logger"))

        self.db_object = None
        self._cursor_ = None

    @property
    def log_info(self):
        return self._print_info

    @property
    def log_warning(self):
        logger = self._kwargs.get("logger")
        if logger and hasattr(logger, "warning"):
            return logger.warning
        return self._print_info

    @property
    def log_error(self):
        return self._print_error

    @property
    def get_schema(self):
        return

    @staticmethod
    @abc.abstractmethod
    def dict_params(k):
        pass

    @property
    def name(self):
        return "BASE"

    def set_logger(self, logger):
        if callable(logger):
            self._print_info = logger
            self._print_error = logger
        if hasattr(logger, "info"):
            self._print_info = logger.info
        if hasattr(logger, "exception"):
            self._print_error = logger.exception

    def __call__(self, *args, **kwargs):
        return self._cursor()

    @staticmethod
    @abc.abstractmethod
    def connect(
            host="127.0.0.1",
            user="root",
            password=None,
            db_name=None,
            port=DEFAULT_PORT,
            file_name=None,
            **kwargs,
    ):
        """
        Making the connexion to the mysql database
        Args:
            host: str, the Mysql server ip
            user: str, the username for mysql connexion
            password: str, the password
            db_name: str, the database name
            port: int,
            file_name:

        Returns: the connexion object reach

        """
        ...

    def reload_connexion(self):
        """
        initialize new connexion to the mysql database

        Returns:

        """
        self.close_connection()
        self.db_object = self.connect(**self._kwargs)

    @abc.abstractmethod
    def _cursor(self):
        return self.db_object.cursor()

    def get_cursor(self):
        """
        Get mysql cursor for making requests
        Returns: mysql.connector.cursor.MySQLCursor, mysql cursor for requests

        """
        try:
            return self._cursor()
        except (Exception, AttributeError):
            self.reload_connexion()
            return self._cursor()

    def close_connection(self):
        """
        close the last connexion establish
        Returns: None

        """
        try:
            self.db_object.close()
        except (AttributeError, Exception):
            pass

    @staticmethod
    @abc.abstractmethod
    def _execute(
            cursor,
            script,
            params=None,
            ignore_error=False,
            connexion=None,
            method="single",
            **kwargs,
    ):
        """
        use to make preparing requests
        Args:
            cursor: cursor object
            script: str, the prepared requests
            params: list|tuple|dict, params for the mysql prepared requests
            ignore_error: bool, if ignore error
            connexion:

        Returns: cursor use for request

        """

    def execute(self, *args, **kwargs):
        ignore_error = kwargs.pop("ignore_error", False)
        try:
            return self._execute(*args, **kwargs, ignore_error=False)
        except Exception as err:  # noqa
            self.rollback()
            if not ignore_error:
                raise err
        return (list(args) + [kwargs.get("cursor")])[0]


    def _is_connected(self):
        return True

    @staticmethod
    def _check_if_cursor_has_rows(cursor):
        return True

    def commit(self):
        try:
            self.db_object.commit()
        except (Exception, AttributeError) as e:
            self._print_error(e)
            pass

    def rollback(self):
        try:
            self.db_object.rollback()
        except (Exception, AttributeError):
            pass

    @staticmethod
    def _remove_quoting_element(script, _quotes_list=('"', "'")):
        ss = ""
        got_quote = 0
        last_quote = None
        current_inner_quote = ""
        quotes = {}
        quote_index = 0
        last_was_escape = False
        _inject_text = "quote_"
        while _inject_text in script:
            _inject_text += "_"

        for index, c in enumerate(script):
            if (
                    (
                            c in _quotes_list and last_quote is None
                    )
                    or c == last_quote
            ) and not last_was_escape:
                if last_quote is None:
                    last_quote = c
                if got_quote > 0:
                    # end of quote
                    got_quote -= 1
                    if got_quote == 0:
                        q = _inject_text + str(quote_index)
                        quotes[q] = (
                                "%s" % last_quote
                                + current_inner_quote
                                + "%s" % last_quote
                        )
                        ss += "{%s}" % (q,)
                else:
                    got_quote += 1
                    quote_index += 1
                    current_inner_quote = ""
            else:
                if got_quote:
                    current_inner_quote += c
                else:
                    last_quote = None
                    ss += c
            if c == "\\":
                last_was_escape = not last_was_escape
            else:
                last_was_escape = False

        return ss, quotes, _inject_text

    @staticmethod
    def _parse_params_no_dict(params, script):
        ss, *_ = BaseDB._remove_quoting_element(script)
        if params is None:
            pass
        elif isinstance(params, (tuple, list)):
            if len(params):
                if isinstance(params[0], dict):
                    final_res = []
                    for p in params:
                        temp = {}
                        for k in p:
                            if (":" + str(k) not in ss) or (
                                    f"%({k})" not in ss
                            ):
                                if not isinstance(temp, dict):
                                    temp.append(p[k])
                                else:
                                    temp[k] = p[k]
                                    temp = list(temp.values())
                            else:
                                if isinstance(temp, dict):
                                    temp[k] = p[k]
                                else:
                                    temp.append(p[k])
                        final_res.append(temp)
                    params = final_res
                params = tuple(params)
        elif isinstance(params, dict):
            if len(params):
                if any((":" + k in ss or f"%({k})" in ss) for k in params):
                    pass
                else:
                    params = list(params.values())
        else:
            params = (params,)
        return params

    def last_insert_rowid_logic(self, cursor=None, table_name=None):
        return cursor

    def insert(self, value: dict, table_name, cur=None, retrieve_id=False):
        part_vars = [str(k) for k in value.keys()]

        xx, value = self.prepare_insert_data(value)

        # value = [v if v is not None else "null" for v in value.values()]
        script = (
                "INSERT INTO "
                + str(table_name)  # nosec
                + " ( "
                + ",".join(part_vars)  # nosec
                + ") VALUES ( "
                + ", ".join(xx)  # nosec
                + " ) "
        )
        if self.name.upper() == "POSTGRES" and retrieve_id:
            script += " RETURNING id"
        if cur is None:
            cursor = self.get_cursor()
        else:
            cursor = cur
        return_object = cursor
        self.execute(cursor, script, params=value)
        if retrieve_id:
            if self.name == "MysqlDB":
                return_object = cursor.lastrowid
            else:
                cursor = self.last_insert_rowid_logic(cursor, table_name)
                return_object = self.get_all_data_from_cursor(cursor, limit=1)
                if isinstance(return_object, (list, tuple)):
                    return_object = (
                        0 if not len(return_object) else return_object[0]
                    )
        if cur is None:
            self.commit()
        return return_object

    def insert_many(
            self, data: list | pandas.DataFrame | str, table_name, **kwargs
    ):

        cursor = self.get_cursor()
        dataset = DatasetFactory(data, **kwargs).dataset.reset_index(drop=True)
        size = dataset.shape[0]
        if not size:
            return

        first_value = dataset.loc[dataset.index[0]].to_dict()
        part_vars = [str(k) for k in first_value.keys()]

        xx, _ = self.prepare_insert_data(first_value)
        script = (
                "INSERT INTO "
                + str(table_name)  # nosec
                + " ( "
                + ",".join(part_vars)  # nosec
                + ") VALUES ( "
                + ", ".join(xx)  # nosec
                + " ) "
        )
        total_tqdm = int(dataset.shape[0] / self.MAX_BUFFER_INSERTING_SIZE) + 2
        for t, buffer in tqdm(
                get_buffer(dataset, max_buffer=self.MAX_BUFFER_INSERTING_SIZE),
                total=total_tqdm,
        ):
            # print(buffer)
            buffer = (
                buffer.astype(object)
                .where(pandas.notnull(buffer), None)
                .to_dict("records")
            )
            # buffer = buffer.astype(object).
            # replace(DatasetFactory.NAN, None).to_dict("records")
            try:
                self.execute(
                    cursor,
                    script,
                    params=[
                        {
                            k: v if not pandas.isnull(v) else None
                            for k, v in row.items()
                        }
                        for row in buffer
                    ],
                    method="many",
                )
                # self.commit()
            except Exception as ex:

                self._print_error(
                    "\n", "->Got error with the buffer: ", buffer
                )
                DatasetFactory(buffer).dataset.to_csv("error.csv", index=False)

                self._print_error(ex)
                return
        self.commit()

    @staticmethod
    def prepare_insert_data(data):
        return ["?" for _ in data], list(data.values())

    @staticmethod
    def _fetchone(cursor, limit=INFINITE):
        index_data = 0
        while index_data < limit:
            row = cursor.fetchone()
            if not row:
                break
            index_data += 1
            yield row

    @staticmethod
    @abc.abstractmethod
    def _get_cursor_description(cursor):
        ...  # noqa: E704

    def get_all_data_from_cursor(
            self, cursor, limit=INFINITE, dict_res=False, export_name=None,
            sep=";"
    ):
        try:
            if not self._check_if_cursor_has_rows(cursor):
                return None
            columns = self._get_cursor_description(cursor).columns
            assert columns is not None
        except (AssertionError, Exception):
            return None
        self.LAST_REQUEST_COLUMNS = columns

        data = []
        try:
            export_file = type(
                "MyTempFile",
                (),
                {"__enter__": lambda *args: 1, "__exit__": lambda *args: 1},
            )()
            if callable(export_name):
                pass
            elif export_name is not None:
                export_file = open(export_name, "w", newline="")

            with export_file:
                writer = None
                if export_name is not None and not callable(export_name):
                    writer = csv.writer(export_file, delimiter=sep)
                    writer.writerow(columns)

                for row in self._fetchone(cursor, limit=limit):
                    if not row:
                        break
                    if self.name == "POSTGRES":
                        row = tuple(
                            [
                                (
                                    float(f)
                                    if isinstance(f, decimal.Decimal)
                                    else f
                                )
                                for f in row
                            ]
                        )
                    if dict_res and export_name is None:
                        row = dict(zip(columns, row))
                    if callable(export_name):
                        export_name(row, columns)
                    elif export_name is not None:
                        writer.writerow(row)
                    else:
                        data.append(row)
            if export_name is not None:
                return
        except Exception:  # nosec
            pass
        if limit == 1:
            if len(data):
                return data[0]
            return None
        return data

    def run_as_batch(
            self,
            script,
            params=None,
            *,
            limit=INFINITE,
            ignore_error=False,
            dict_res=False,
            batch_size=None,
    ):
        batch_size = int(batch_size or self.MAX_BUFFER_INSERTING_SIZE)
        cursor = self.run_script(
            script,
            params=params,
            retrieve=False,
            limit=batch_size,
            ignore_error=ignore_error,
            dict_res=dict_res,
            _for_batch=True,
        )
        size = 0
        while size < limit:
            data = self.get_all_data_from_cursor(
                cursor, limit=batch_size, dict_res=dict_res
            )
            size += len(data)
            if size > limit:
                data = data[: batch_size - (size - limit)]
            if not len(data):
                return
            yield data
        return

    def run_script(
            self,
            script,
            params=None,
            *,
            retrieve=True,
            limit=INFINITE,
            ignore_error=False,
            dict_res=False,
            export=False,
            export_name=None,
            sep=";",
            _for_batch=False,
    ):
        try:
            assert os.path.exists(script)
            with open(script) as file:
                script = file.read().strip()
        except (AssertionError, OSError, Exception):
            pass
        if limit is None:
            limit = INFINITE
        if not self._is_connected():
            self.reload_connexion()
        cursor = self.get_cursor()
        try:
            cursor = self.execute(
                cursor,
                script,
                params=params,
                ignore_error=False,
                connexion=self.db_object,
            )
        except Exception as ex:
            self.LAST_REQUEST_COLUMNS = None
            # self.rollback()
            if not ignore_error:
                raise Exception(ex)
            else:
                self._print_error(ex)
            return
        self.commit()
        if _for_batch:
            return cursor

        if export:
            if export_name is None:
                export_name = os.path.join(
                    os.path.join(os.environ["USERPROFILE"]), "Downloads"
                )
                if not os.path.exists(export_name):
                    export_name = os.getcwd()
                export_name = os.path.join(export_name, "export_data.csv")
                export_name = get_no_filepath(export_name)

        if retrieve:
            data = self.get_all_data_from_cursor(
                cursor,
                limit=limit,
                dict_res=dict_res,
                export_name=export_name,
                sep=sep,
            )
            if export_name is not None:
                return export_name
            if dict_res:
                if limit == 1:
                    return Cdict(data)
                return [Cdict(d) for d in data]
            return data


if __name__ == "__main__":
    pass
