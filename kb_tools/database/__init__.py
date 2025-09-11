# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib
import os
from urllib.parse import urlparse
import re

from kb_tools.database.basedb import BaseDB, Cdict

_fields_types_reg = {
    "postgresdb": (
        "bigint", "bigserial", r"bit(?:\s*\(\s*\d+\s*\))?\b", r"bit\s+varying",
        r"bool(?:ean)?\b", "box", "bytea",
        r"char(?:acter)?(?:\s*\(\s*\d+\s*\))?",
        r"character\s+varying(?:\s*\(\s*\d+\s*\))?", "cidr", "circle",
        r"date\b", r"double\s+precision", "inet", "integer", r"json\b",
        "jsonb", "line", "smallint", "real", "smallserial", r"serial\d?\b",
        "text", "uuid",r"varbit(?:\s*\(\s*\d+\s*\))?",
        r"time(?:tz)?\b(?:\s*\(\s*\d+\s*\))?(?:\s*without\s+time\s+zone)?",
        r"timestamp(?:tz)?(?:\s*\(\s*\d+\s*\))?(?:\s*without\s+time\s+zone)?",
        r"numeric(?:\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?", # a voir
        r"decimal(?:\s*\(\s*\d+\s*(?:,\s*\d+\s*)?\))?",  # a voir
        r"int\d?\b", r"float\d?\b", r"varchar(?:\s*\(\s*\d+\s*\))?",

    )
}

_container_extra = {
    "postgresdb": [("'", "'"), ("(", ")")]
}

_modify_column_type = {
    "postgresdb": "ALTER TABLE {table} "
                  "ALTER COLUMN {column} TYPE {type}",
    "mysqldb": "ALTER TABLE {table} "
               "MODIFY ({column} {type})"
}

def _is_some_type_equal_another(type1, type2):
    type1 = re.sub(
        r"\s\s+", " ",
        re.sub(r"(\W)\s+", r"\1", re.sub(r"\s+(\W)", r"\1", type1))
    ).lower()
    type2 = re.sub(
        r"\s\s+", " ",
        re.sub(r"(\W)\s+", r"\1", re.sub(r"\s+(\W)", r"\1", type2))
    ).lower()
    _t = {"1": type1, "2": type2}
    for k, v in list(_t.items()):
        if v.startswith("character varying"):
            _t[k] = "varchar" + v.split("character varying")[1]
        elif v.startswith("character"):
            _t[k] = "char" + v.split("character")[1]
        elif v.startswith("int"):
            _t[k] = "integer"
        elif v in ("float", "real", "double precision"):
            _t[k] = "real"
        elif v.startswith(("bool",)):
            _t[k] = "boolean"
        elif v.startswith(("bit varying",)):
            _t[k] = "varbit" + v.split("bit varying")[1]
        if v.startswith("timestamp"):
            _t[k] = "timestamp"

    return _t["1"] == _t["2"]


class DataManager:
    def __new__(cls, uri=None, **kwargs) -> "BaseDB":
        _kwargs = kwargs.copy()
        _schema = kwargs.pop("schema", None)
        if uri is None:
            uri = kwargs

        if isinstance(uri, dict):
            uri = {k.lower(): uri[k] for k in uri}

            username = uri.get("user", "root")
            password = uri.get("pwd", "") or uri.get("password", "")
            host = uri.get("host", "127.0.0.1")
            port = uri.get("port")
            database_name = uri.get("db_name")
            file_name = uri.get("file_name") or ":memory:"
            if uri.get("file_name"):
                sgbd_name = "sqlite"
            else:
                sgbd_name = uri.get("sgbd_name")
        else:
            assert isinstance(uri, str), "Bad URI value given"
            uri = cls.parse_uri(uri)
            username, password = uri["username"], uri["password"]
            host, port, sgbd_name = uri["host"], uri["port"], uri["drivername"]
            database_name, file_name = uri["database"], uri["file_name"]
            uri = {}

        assert sgbd_name, "Required argument sgbd_name"
        sgbd_name = next(
            filter(
                lambda x: x.lower().split("db.py")[0] in sgbd_name.lower(),
                os.listdir(os.path.dirname(__file__)),
            )
        ).split(".")[0]

        module = importlib.import_module(
            "." + sgbd_name, package="kb_tools.database"
        )
        class_name = next(
            filter(lambda x: sgbd_name.lower() == str(x).lower(), dir(module))
        )
        class_object = getattr(module, class_name)

        _kwargs.update(uri)

        _kwargs.update(
            {
                "host": host,
                "user": username,
                "password": password,
                "db_name": database_name,
                "port": port,
                "file_name": file_name,
                "_schema": _schema
            }
        )
        db_object: BaseDB = class_object(**_kwargs)

        if os.getenv("kb_tools_MIGRATION") and not os.getenv(
                "kb_tools_MIGRATION_MANUALLY"):
            cls.init_db(db_object, sgbd_name)

        return db_object

    @staticmethod
    def _check_prev_name(comment):
        if comment:
            line_comment, = comment.groups()

            previous_name = re.search(
                r"prev(?:ious)?[\s|_]+"
                r"(?:name|columns|field|table)\s*:\s*(\w+)",
                line_comment, flags=re.I
            )

            if previous_name:
                previous_name, = previous_name.groups()
                return previous_name

    @staticmethod
    def _parse_sql_db_creation_script(script, sgbd_name="postgresdb"):
        origin_script = script
        _comment = {
            "postgresdb": {
                "single_line": "--",
                "multi_line": ("/*", "*/")
            },
            "sqlitedb":{
                "single_line": "--",
                "multi_line": ("/*", "*/")
            }
        }.get(sgbd_name)
        # check for quoted string

        if _comment:
            single_line = _comment["single_line"]
            multi_line = _comment["multi_line"]
            script = re.sub(
                rf"{re.escape(single_line)}.*$",
                "", script, flags=re.M
            )
            script = re.sub(
                rf"{re.escape(multi_line[0])}.*?{re.escape(multi_line[1])}",
                "", script, flags=re.S
            )
        script = script.strip() + ";"
        parts = re.split(
            r"(create\s+table(?:\s+if\s+not\s+exists)?\s+\w+\s*\(.*?\)\s*;)",
            script, flags=re.I | re.S)
        schema = {}
        if sgbd_name in _fields_types_reg:
            _types = "|".join(
                r"%s" % (t,) for t in _fields_types_reg[sgbd_name])
        else:
            _types = r"\w+(?:\s*\(\s*\d+\s*\))?"

        _container = (_container_extra.get(sgbd_name) or
                      [("'", "'"), ("(", ")"), ("\"", "\"")])
        for part in parts:
            m = re.match(
                r"^create\s+table(?:\s+if\s+not\s+exists)?\s+"
                r"(\w+)\s*\((.*?)\)\s*;",
                part, flags=re.I | re.S)
            if m:
                table_name, content = m.groups()
                table_last_name = None

                if "postgres" in sgbd_name:
                    content = re.sub(
                        r"\binteger\s+primary\s+key\s+auto_?increment\b",
                        "SERIAL primary key", content, flags=re.I)
                    content = re.sub(
                        r"\bauto_?increment\b",
                        "SERIAL", content, flags=re.I)

                if _comment:
                    _line = _comment["single_line"]
                    line_comment = re.search(
                        rf"create\s+table.*?{table_name}\s*\(?\s*"
                        rf"{_line}([^\n]+)$", origin_script,
                        flags=re.I | re.M | re.S
                    )
                    table_last_name = DataManager._check_prev_name(
                        line_comment
                    )
                _columns = list(re.finditer(
                    rf"(\w+)\s+({_types})(?:\w*?)?(\s+not\s+null)?"
                    r"(\s+primary\s+key)?(.*?),",
                    content + ",", flags=re.S | re.I))
                final_columns = []
                for i, _match in enumerate(_columns):
                    c = _match.groups()
                    extra = c[4].strip()
                    is_primary_key = c[3]

                    if i + 1 < len(_columns):
                        extra += (
                                "," +
                                content[_match.end():_columns[i+1].start()]
                        ).strip()
                        if extra.endswith(","):
                            extra = extra[:-1]

                    _cur = ""
                    default = ""
                    _s_cont = ""

                    for _car in extra:
                        _is_new_line = re.match(r"\s", _car) is not None
                        if _cur.lower() == "default":
                            if _is_new_line and default == "":
                                continue
                            if not _s_cont and _is_new_line:
                                _cur = ""
                                _s_cont = ""
                                break
                            for cont in _container:
                                if _car == cont[0]:
                                    if cont[0] == cont[
                                        1] and _s_cont and _car == _s_cont[-1]:
                                        _s_cont = _s_cont[:-1]
                                        break
                                    _s_cont += cont[1]
                                    break
                                elif _s_cont and _car == _s_cont[-1]:
                                    _s_cont = _s_cont[:-1]
                                    break
                            default += _car
                            continue
                        if _is_new_line:
                            _cur = ""
                            continue
                        _cur += _car

                    previous_name = None
                    if _comment:

                        _line = _comment["single_line"]

                        line_comment = re.search(
                            rf"create\s+table.*?{table_name}\s*\("
                            rf".*?{c[0]}[^\n]+{_line}([^\n]+)$", origin_script,
                            flags=re.I | re.M | re.S
                        )
                        previous_name = DataManager._check_prev_name(
                            line_comment
                        )
                    #
                    final_columns.append(
                        {
                            "col_name": c[0],
                            "previous_name": previous_name,
                            "type": c[1],
                            "not_null": (c[2] not in ("", None)),
                            "is_primary_key": is_primary_key not in ("", None),
                            "column_default": default.strip(),
                            "extra": (
                                    (c[2] or "") + " " +
                                    (is_primary_key or "") + " " +
                                    extra
                            ).strip()
                        }
                    )

                schema[table_name] = {
                    "content": content,
                    "columns": final_columns,
                    "previous_name": table_last_name
                }
        return schema

    @staticmethod
    def init_db(
            db_object: BaseDB, sgbd_name=None, _command="upgrade", logger=None,
            _cache_file=None, _to_ignore=()
        ):

        assert _command in ("upgrade", "migrate")
        if sgbd_name is None:
            sgbd_name = db_object.__class__.__name__.lower()

        _upgrade_candidate = ""
        if _command == "migrate":
            if _cache_file is None:
                _cache_file = "migrations/_cache"
            os.makedirs(os.path.dirname(_cache_file), exist_ok=True)
            _cache_file = open(_cache_file, "w")
        else:
            _to_ignore = _to_ignore or ()
            if os.path.exists(str(_cache_file)):
                with open(str(_cache_file)) as _fp:
                    _upgrade_candidate = _fp.read()
                os.remove(_cache_file)

        final_schema = getattr(db_object, "_kwargs").get("_schema")

        if not isinstance(final_schema, str):
            return
        if os.path.exists(final_schema):
            with open(final_schema, encoding="utf-8") as _fp:
                final_schema = _fp.read()

        if logger:
            db_object.set_logger(logger)

        increment = 0
        def _execute(_script):
            nonlocal increment
            increment += 1
            _log = f"{increment}: {repr(_script)}\n"
            if _command == "upgrade":
                if increment in _to_ignore:
                    db_object.log_info("Ignoring:: ", _log[:-1])
                    return
                if _upgrade_candidate:
                    if _log not in _upgrade_candidate:
                        message = ("Got different schema need to"
                                    " rerun command migrate")
                        db_object.log_info(message)
                        raise ValueError(message)

                db_object.log_info(_script)
                db_object.run_script(_script)
            else:
                _cache_file.write(_log)
                db_object.log_info(_log[:-1])

        final_schema = DataManager._parse_sql_db_creation_script(
            final_schema, sgbd_name=sgbd_name
        )
        db_object.auto_commit = False
        current_schema = db_object.get_schema
        current_tables = set(c["tableName"] for c in current_schema)

        def _update_field(_last, _new, _table):
            if sgbd_name == "sqlitedb":
                # alter table is limited for sqlitedb
                if not _is_some_type_equal_another(
                        _last["type"], _new["type"]
                ):
                    db_object.log_warning("Auto migration for sqlitedb "
                                          "cannot modify field type. "
                                          "needed: (%s.%s) from %s to %s" % (
                        _table, _new['col_name'], _last['type'], _new['type']
                    ))
                return
            _s = (
                _modify_column_type.get(sgbd_name) or
                _modify_column_type.get("mysqldb")
             )
            if not _is_some_type_equal_another(_last["type"], _new["type"]):
                if (
                        _new['type'].lower().startswith("serial")
                        and _last['type'].lower().startswith("int")
                ):
                    return
                _execute(
                    str(_s).format(
                        table=_table,
                        type=_new['type'] + (
                            " SET " if sgbd_name.startswith("postgres")
                            else ""
                        )+
                             (

                            " NOT NULL" if _new.get("not_null") else ""
                        ),
                        column=_new['col_name']
                    )
                )
            if _new["column_default"]:
                if (
                        (_new["column_default"] or "").lower() !=
                        (_last["column_default"] or "").lower()
                ):
                    _execute(
                        f"ALTER TABLE {_table} "
                        f"ALTER COLUMN {_new['col_name']} "
                        f"SET DEFAULT {_new['column_default']}"
                    )
            elif _last["column_default"]:
                _execute(
                    f"ALTER TABLE {_table} "
                    f"ALTER COLUMN {_new['col_name']} "
                    f"DROP DEFAULT"
                )

        try:
            table_got = []
            for table, values  in final_schema.items():
                current_columns = {
                    c["columnName"].lower(): Cdict({
                        "col_name": c["columnName"],
                        **c
                    })
                    for c in current_schema if
                    c["tableName"] == table
                }

                if table not in current_tables:
                    if (
                            values.get("previous_name") and
                            values["previous_name"] in current_tables
                    ):
                        # need to rename table previous_name to table
                        _execute(
                            f"ALTER TABLE {values['previous_name']} "
                            f"RENAME TO {table}"
                        )
                        current_columns = {
                            c.lower(): c
                            for c in current_schema if
                            c["tableName"] == values["previous_name"]
                        }
                        table_got.append(values['previous_name'])
                    else:
                        # got no existing table: create it
                        _execute(
                            f"CREATE TABLE {table}({values['content']})"
                        )
                        continue
                else:
                    table_got.append(table)
                # table exists loop for each column
                col_got = []
                for f_col in values["columns"]:
                    col_name, col_type, default, is_primary_key, extra = (
                        f_col["col_name"], f_col["type"],
                        f_col["column_default"], f_col["is_primary_key"],
                        f_col["extra"]
                    )

                    if col_name.lower() not in current_columns:
                        _prev_name = (
                                f_col.get("previous_name") or ""
                        ).lower()
                        if (
                                _prev_name in current_columns
                        ):
                            # need to rename
                            _execute(
                                f"ALTER TABLE {table} "
                                f"RENAME COLUMN {f_col['previous_name']} "
                                f"TO {col_name}"
                            )
                            col_got.append(_prev_name)
                            _update_field(
                                current_columns[_prev_name],
                                f_col,
                                table
                            )
                            continue
                        _col = f"{col_name} {col_type} {extra}"
                        # got new columns create it
                        _execute(
                            f"ALTER TABLE {table} ADD COLUMN {_col}"
                        )
                    else:
                        # column exists
                        col_got.append(col_name.lower())
                        _exist_col = current_columns[col_name.lower()]
                        _update_field(_exist_col, f_col, table)

                for col in current_columns:
                    if col not in col_got:
                        _execute(
                            f"ALTER TABLE {table} "
                            f"DROP COLUMN {col}")

            # for table in current_tables:
            #     if table not in table_got:
            #         _execute(
            #             f"DROP TABLE {table}",
            #         )

        except Exception as err:
            # db_object.rollback()
            db_object.log_error(err)
            raise err
        finally:
            if _cache_file:
                _cache_file.close()

        db_object.set_logger(getattr(db_object, "_kwargs").get("logger"))
        db_object.commit()
        db_object.auto_commit = True

    @staticmethod
    def parse_uri(uri: str, **kwargs):
        assert isinstance(uri, str), "Bad URI value given"
        res = urlparse(uri)
        if str(res.scheme).lower() not in ("c", "", "sqlite"):
            file_name = ":memory:"
            sgbd_name, username, password, host, port, database_name = [
                (kwargs.get(key) or getattr(res, key))
                for key in [
                    "scheme",
                    "username",
                    "password",
                    "hostname",
                    "port",
                    "path",
                ]
            ]
            if database_name.startswith(("/", "\\")):
                database_name = database_name[1:]
        else:
            if res.path:
                uri = res.path
                if uri.startswith(("/", "\\")):
                    uri = uri[1:]

            sgbd_name = "sqlite"
            try:
                open(uri, "x").close()
            except FileExistsError:
                pass
            except OSError:
                raise ValueError("Bad file path given")
            file_name = uri
            username, password, host, port, database_name = (
                None,
                None,
                None,
                None,
                None,
            )
        return {
            "username": username,
            "password": password,
            "host": host,
            "database": database_name,
            "port": port,
            "drivername": sgbd_name,
            "file_name": file_name
        }


if __name__ == "__main__":
    pass


