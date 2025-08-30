# -*- coding: utf-8 -*-
import re

import psycopg2

from kb_tools.database.basedb import BaseDB
from kb_tools.tools import Cdict


def parse_script(script, params=None, _quotes_list=('"', "'")):
    if "like" not in script.lower():
        return script, params
    ss, quotes, _inject_text = getattr(BaseDB, "_remove_quoting_element")(
        script, _quotes_list
    )

    reg = re.finditer(r"\s+like\s*\{(%s\d+)}" % _inject_text, ss, flags=re.I)
    try:
        assert ss % (1,) == ss
        _not_match_dict_params = True
    except AssertionError:
        _not_match_dict_params = True
    except ValueError as error:
        _not_match_dict_params = "incomplete" in str(error)
    except TypeError as error:
        _not_match_dict_params = not ("requires a mapping" in str(error))
    except:  # noqa E722
        _not_match_dict_params = True

    for r in reg:
        (quote,) = r.groups()
        if "%" in quotes[quote] and quotes[quote][0] == "'":
            # may consider: unsupported format character ''' (0x27)
            if _not_match_dict_params:
                index = ss.split("{%s}" % quote)[0].count("%s")
                if not params:
                    params = []
                params = list(params)
                params.insert(index, quotes.pop(quote)[1:-1])
            else:
                if not params:
                    params = {}
                params[quote] = quotes.pop(quote)[1:-1]
            ss = ss.replace(
                "{%s}" % quote,
                "%s" if _not_match_dict_params else f"%({quote})s",
            )

    return ss.format(**quotes), params


class PostgresDB(BaseDB):
    DEFAULT_PORT = 5432

    def __init__(self, **kwargs):
        if not kwargs.get("port"):
            kwargs["port"] = self.DEFAULT_PORT
        super().__init__(**kwargs)

    @staticmethod
    def dict_params(k):
        return f"%({k})s"

    @property
    def get_schema(self):
        fields = self.run_script(
            """
            WITH fields AS (
                SELECT
                    column_name AS columnName,
                    data_type AS type,
                    table_name AS tableName,
                    column_default AS columnDefault,
                    CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END nullable
                FROM information_schema.columns
            ),
            primary_field AS (
                SELECT   
                  pg_attribute.attname field, 
                  pg_class.relname AS table_name
                FROM pg_index, pg_class, pg_attribute, pg_namespace 
                WHERE 
                  indrelid = pg_class.oid AND 
                  nspname = 'public' AND 
                  pg_class.relnamespace = pg_namespace.oid AND 
                  pg_attribute.attrelid = pg_class.oid AND 
                  pg_attribute.attnum = any(pg_index.indkey)
                 AND indisprimary
            ),
            foreign_key_table AS (
                SELECT 
                    tc.table_name, 
                    kcu.column_name AS field, 
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name 
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
            ),
            final_field AS (
                SELECT 
                    f.*, 
                    CASE WHEN p.field IS NULL THEN 0 ELSE 1 END
                        AS is_primary_key,
                    ft.foreign_table_name,
                    ft.foreign_column_name
                FROM fields f LEFT JOIN primary_field p
                    ON f.columnName = p.field AND f.tableName = p.table_name
                    LEFT JOIN foreign_key_table ft ON
                    f.columnName = ft.field AND f.tableName = ft.table_name
            )
            SELECT
                f.*
            FROM final_field f,
                (
                    SELECT table_name tableName FROM information_schema.tables
                    WHERE table_type = 'BASE TABLE'
                        AND table_schema NOT IN
                            ('pg_catalog', 'information_schema')
                ) tables
            where tables.tableName=f.tableName
        """,
            dict_res=True,
        )
        for f in fields:
            default = str(f["columnDefault"]).lower()
            if re.match("nextval\(.*\)", default):
                f.columnDefault = None
            elif re.match("null(::[\w\s]*)?", default):
                f.columnDefault = None
        return fields

    def _is_connected(self):
        try:
            return not self.db_object.closed
        except (AttributeError, psycopg2.Error, Exception):
            return False

    @staticmethod
    def connect(
        host="127.0.0.1",
        user="root",
        password=None,
        db_name=None,
        port=DEFAULT_PORT,
        **kwargs,
    ) -> psycopg2._psycopg.connection:
        """
        Making the connexion to the mysql database
        Args:
            host: str, the Mysql server ip
            user: str, the username for mysql connexion
            password: str, the password
            db_name: str, the database name
            port:

        Returns: psycopg2.connection object

        """
        try:
            return psycopg2.connect(
                host=host,
                user=user,
                dbname=db_name,
                password=password,
                port=port,
            )
        except Exception as ex:
            ex.args = [
                "Database connexion fail"
                + ("" if not len(ex.args) else (" --> " + str(ex.args[0])))
            ] + list(ex.args[1:])
            raise ex

    @staticmethod
    def _execute(
        cursor: psycopg2._psycopg.cursor,
        script,
        params=None,
        ignore_error=False,
        connexion=None,
        **kwargs,
    ):
        """
        use to make preparing requests
        Args:
            cursor:
            script: str, the prepared requests
            params: list|tuple|dict, params for the posgresql prepared requests
            connexion:

        Returns: the cursor after make request

        """
        params = BaseDB._parse_params_no_dict(params, script)
        method = "execute" + ("many" if kwargs.get("method") == "many" else "")
        script, params = parse_script(script, params=params)
        if isinstance(params, (tuple, list)):
            params = tuple(params)
        elif isinstance(params, dict):
            pass
        else:
            params = (params,)
        try:
            getattr(cursor, method)(script, params)
            return cursor
        except Exception as ex:
            if ignore_error:
                return None
            raise ex

    def _cursor(self) -> psycopg2._psycopg.cursor:
        return self.db_object.cursor()

    @staticmethod
    def _get_cursor_description(cursor):
        return Cdict(columns=[desc[0] for desc in cursor.description or []])

    @staticmethod
    def prepare_insert_data(data: dict):
        return ["%s" for _ in data], list(data.values())

    @property
    def name(self):
        return "POSTGRES"


if __name__ == "__main__":
    pass
