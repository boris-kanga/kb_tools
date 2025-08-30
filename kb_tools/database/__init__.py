# -*- coding: utf-8 -*-
from __future__ import annotations

import importlib
import os
from urllib.parse import urlparse

from kb_tools.database.basedb import BaseDB


class DataManager:
    def __new__(cls, uri=None, **kwargs) -> "BaseDB":

        _kwargs = kwargs.copy()
        if uri is None:
            uri = kwargs
        if not uri:
            for k, v in os.environ.items():
                if "uri" in k.lower():
                    uri = v
                    break

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
            }
        )
        db_object = class_object(**_kwargs)

        cls.init_db(db_object, sgbd_name)

        return db_object

    @staticmethod
    def init_db(db_object, sgbd_name=None):
        pass

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
            uri = res.path[1:] or uri
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
