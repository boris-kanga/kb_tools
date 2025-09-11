# -*- coding: utf-8 -*-
import sqlite3
import threading

from kb_tools.database.basedb import BaseDB
from kb_tools.tools import Cdict


class SQLiteDB(BaseDB):
    def __init__(self, **kwargs):
        if not kwargs.get("file_name"):
            kwargs["file_name"] = ":memory:"
        self._thread_name = threading.current_thread().name
        super().__init__(**kwargs)

    def close_connection(self):
        if self._is_connected():
            super().close_connection()

    @property
    def get_schema(self):
        return self.run_script(
            """
            WITH tables AS
                (SELECT
                    name tableName
                FROM sqlite_master WHERE type = 'table' AND
                    tableName NOT LIKE 'sqlite_%'),
            foreign_key_table AS (
                SELECT 
                m.name AS table_name,
                p."from" as field,
                p."table" AS foreign_table_name,
                p."to" AS foreign_column_name
            FROM
                sqlite_master m
                JOIN pragma_foreign_key_list(m.name) p ON m.name != p."table"
            )
            SELECT
                fields.name AS columnName,
                fields.type,
                tableName,
                dflt_value AS columnDefault,
                "notnull" AS nullable,
                fields.pk AS is_primary_key,
                ft.foreign_table_name,
                ft.foreign_column_name
            FROM
                tables CROSS JOIN
                    pragma_table_info(tables.tableName) fields
                LEFT JOIN foreign_key_table ft
                    ON fields.name=ft.field 
                    AND tables.tableName=ft.table_name
            """,
            dict_res=True,
        )

    def last_insert_rowid_logic(self, cursor=None, table_name=None):
        if table_name is not None:
            table_name = " FROM " + str(table_name)
        else:
            table_name = ""
        return self._execute(cursor, "select last_insert_rowid()" + table_name)

    @staticmethod
    def _execute(
        cursor,
        script,
        params=None,
        ignore_error=False,
        method="single",
        **kwargs
    ):
        """
        use to make preparing requests
        Args:
            cursor: mysql.connector.cursor.MySQLCursor
            script: str, the prepared requests
            params: list|tuple|dict, params for the mysql prepared requests
            ignore_error: bool

        Returns: the receipt cursor

        """
        if method == "many":
            method = "executemany"
        else:
            method = "execute"
        args = [
            script.replace("%s", "?")
            .replace("COALESCE", "ISNULL")
            .replace("CURRENT_DATE", "DATE()")
        ]
        if params is None:
            pass
        elif isinstance(params, (tuple, list)):
            if len(params):
                if isinstance(params[0], dict):
                    final_res = []
                    for p in params:
                        temp = {}
                        for k in p:
                            if ":" + str(k) not in script:
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
                args.append(params)
        elif isinstance(params, dict):
            if len(params):
                k = list(params.keys())[0]
                if ":" + str(k) in script:
                    pass
                else:
                    params = list(params.values())
                args.append(params)
        else:
            params = (params,)
            args.append(params)
        try:

            getattr(cursor, method)(*args)
            return cursor
        except Exception as ex:
            # print(params, script)
            print(script, params)
            if ignore_error:
                return None
            raise ex

    def _is_connected(self):
        return threading.current_thread().name == self._thread_name

    def _cursor(self):
        return self.db_object.cursor()

    @staticmethod
    def connect(file_name="database.db", **kwargs) -> sqlite3.Connection:
        """
        Making the connexion to the mysql database
        Args:
            file_name: str, file name path
        Returns: the connexion object reach

        """
        try:
            return sqlite3.connect(file_name)
        except Exception as ex:
            ex.args = [
                "Database connexion fail"
                + ("" if not len(ex.args) else (" --> " + str(ex.args[0])))
            ] + list(ex.args[1:])
            raise ex

    @staticmethod
    def _get_cursor_description(cursor):
        return Cdict(columns=[desc[0] for desc in cursor.description or []])

    @staticmethod
    def prepare_insert_data(data: dict):
        return ["?" for _ in data], list(data.values())

    @staticmethod
    def dict_params(k):
        return ":" + str(k)

    @property
    def name(self):
        return "SQLITE"
