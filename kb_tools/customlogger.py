# -*- coding: utf-8 -*-
"""
CustomLogger object. Use for logging information
"""

import datetime
import io
import logging
import os
import sys
import traceback
import re

import shutil

from kb_tools.tools import get_no_filepath


def _now():
    return datetime.datetime.now(tz=datetime.timezone.utc)


class CustomLogger:
    _CURRENT_LOGGER = None
    def __init__(
            self,
            name,
            log_dir=None,
            file_log_format=(
                "%(asctime)s %(name)-8s %(levelname)-8s %(message)s"
            ),
            **kwargs
    ):
        """
        Constructor of custom logger
        Examples:
            For max_size = 2 Ko==2 * 1024 bytes.
            >>> logger = CustomLogger("test", max_size=2 * 1024, \
                        base_file_name="test.txt")
            >>> logger.info("test")
        Args:
            name: str, the log name
            log_dir: log_dir: str|path, the path of the log if it was going to
                use log_file
            file_log_format: str, the format of log display
            **kwargs: dict, it can include params
                console: bool, default False
                each: str,
                console_log_format: str, default '%(name)-8s %(levelname)-8s
                    %(message)s'
                max_size: float, specify the max size that can reach the logger
                    file, default None (No limit)
                base_file_name: str, default os.path.join(log_dir, name,
                    ".txt"), use to specify the name style of log file.
                callback: fonction for callback after logging
                callback_each_logging: bool,
        """
        self.name = name
        self.file_log_format = file_log_format
        self.console = kwargs.get("console", True)
        self.callback = kwargs.get("callback", None)
        self.callback_each_logging = kwargs.get("callback_each_logging", True)
        self._lvl = logging.INFO
        if callable(self.callback):
            self.log_capture_string = io.StringIO()
            self.log_capture_string.name = "<custom-logger-io>"
        else:
            self.log_capture_string = None
            self.callback = None

        self.console_log_format = kwargs.get(
            "console_log_format", "%(name)-8s %(levelname)-8s %(message)s"
        )
        max_size = kwargs.get("max_size", None)
        if max_size:
            kwargs["each"] = max_size
        self.each = kwargs.get("each")
        # parse each
        if isinstance(self.each, str):
            # 1M -> when log file size more than 1M
            _reg = re.match(r"^\s*(\d*)\s*([MGKB])\s*$", self.each, flags=re.I)
            if _reg:
                size, unit = _reg.groups()
                eq = {"b": 1, "k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}[
                    unit.lower()]
                self.each = ("size", int(size or "1") * eq)
            else:
                # 1(days|week|month) -> recreate à other log for each new \1
                _reg = re.match(
                    r"^\s*(\d*)\s*(minutes?|hours?|days?|weeks?|months?)\s*$",
                    self.each, flags=re.I
                )
                if _reg:
                    size, unit = _reg.groups()
                    eq = {
                        "day": 24 * 60 * 60,
                        "week": 7 * 24 * 60 * 60,
                        "month": 30 * 24 * 60 * 60,
                        "minute": 60,
                        "hour": 60 * 60
                    }[unit.split("s")[0].lower()]
                    self.each = ("seconds", int(size or "1") * eq)
                else:
                    self.each = None
        elif isinstance(self.each, float):
            self.each = ("size", self.each)
        else:
            self.each = None
        self._suffix_date_format = "%Y-%m-%d" + (
            "__%H_%M_%S"
            if (
                    self.each is None or
                    self.each[0] != "seconds" or
                    self.each[1] < 24 * 60 * 60
            )
            else ""
        )
        self.base_file_name = kwargs.get(
            "base_file_name",
            (
                None
                if log_dir is None
                else os.path.join(log_dir, str(name) + ".txt")
            ),
        )
        self.extension = None
        self._current_start_date = _now()
        self.writer = None

        if log_dir is not None:
            self.base_file_name = os.path.join(
                log_dir, os.path.basename(self.base_file_name)
            )
        else:
            if self.base_file_name is not None:
                self.base_file_name = os.path.realpath(self.base_file_name)
                log_dir = os.path.dirname(self.base_file_name)

        if self.base_file_name is not None:
            self.base_file_name, self.extension = os.path.splitext(
                self.base_file_name
            )
        if log_dir and not os.path.isdir(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        self._size = 0
        self._create_new_logger_handler()

        if kwargs.get("_set_current_logger", True) and self.console:
            CustomLogger._CURRENT_LOGGER = self

    @classmethod
    def get_current(cls):
        if isinstance(cls._CURRENT_LOGGER, cls):
            return cls._CURRENT_LOGGER
        return cls(logging.root.name)

    def _need_goto_next_file_cursor(self, _datetime=None, _size=None) -> bool:
        log_file = self.base_file_name + self.extension
        if _size is None:
            _size = os.stat(log_file).st_size
        if _datetime is None:
            _datetime = self._current_start_date

        if self.each is None:
            return False

        if self.each[0] == "size":
            return _size > self.each[1]
        elif self.each[0] == "seconds":
            return (
                    _now() - _datetime
            ).total_seconds() > self.each[1]
        return False

    def send_all_logger_message_by_callback(self):
        if isinstance(self.log_capture_string, io.StringIO):
            self.callback(self.log_capture_string.getvalue())
            self.log_capture_string.truncate(0)
            self.log_capture_string.seek(0)

    @staticmethod
    def get_logger(name=None):
        """
        use to got new logger object
        Args:
            name: str

        Returns:
            logging.getLoggerClass(), logger object

        """
        if name is not None:
            return logging.getLogger(name)
        return logging.getLogger()

    def setLevel(self, lvl):
        self._lvl = lvl
        if self.writer:
            try:
                self.writer.setLevel(self._lvl)
            except:  # noqa E722
                pass

    def _set_current_start_date(self):
        self._current_start_date = _now()
        log_file = self.base_file_name + self.extension
        if os.path.exists(log_file) and self.each is not None:
            try:
                _stat = os.stat(log_file)
                _d = datetime.datetime.fromtimestamp(
                    _stat.st_ctime, tz=datetime.timezone.utc
                )
                if sys.platform.startswith("linux"):
                    try:
                        with open(log_file) as fp:
                            first_line = fp.readline()

                            res = re.search(
                                r"^(\d{2}|\d{4})"
                                r"([-/_.]?)(\d{2})\2(\d{2}|\d{4})"
                                r"[-/_\s]*(\d{2})([-:/_ ])"
                                r"(\d{2})\6(\d{2})\b",
                                first_line
                            )
                            _dd = None
                            if not res:
                                res = re.search(
                                    r"^(\d{2})([-:/_ ])(\d{2})"
                                    r"\2(\d{2})"
                                    r"[-/_\s]*"
                                    r"(\d{2}|\d{4})"
                                    r"([-/_.]?)(\d{2})\6(\d{2}|\d{4})\b",
                                    first_line
                                )
                            if res:
                                res = res.groups()
                                if len(res[-1]) == 4:
                                    _dd = datetime.datetime(
                                        year=int(res[7]),
                                        month=int(res[6]),
                                        day=int(res[4]),
                                        hour=int(res[0]),
                                        minute=int(res[2]),
                                        second=int(res[3]),
                                        tzinfo=datetime.timezone.utc
                                    )
                                if len(res[0]) == 4:
                                    _dd = datetime.datetime(
                                        year=int(res[0]),
                                        month=int(res[2]),
                                        day=int(res[3]),
                                        hour=int(res[4]),
                                        minute=int(res[6]),
                                        second=int(res[7]),
                                        tzinfo=datetime.timezone.utc
                                    )
                                if len(res[4]) == 4:
                                    _dd = datetime.datetime(
                                        year=int(res[4]),
                                        month=int(res[6]),
                                        day=int(res[7]),
                                        hour=int(res[0]),
                                        minute=int(res[2]),
                                        second=int(res[3]),
                                        tzinfo=datetime.timezone.utc
                                    )
                                elif len(res[3]) == 4:
                                    _dd = datetime.datetime(
                                        year=int(res[3]),
                                        month=int(res[2]),
                                        day=int(res[0]),
                                        hour=int(res[4]),
                                        minute=int(res[6]),
                                        second=int(res[7]),
                                        tzinfo=datetime.timezone.utc
                                    )

                            if _dd and _dd < _d:
                                _d = _dd
                    except ValueError:
                        pass
                    except Exception as e:
                        if "file" in str(e).lower():
                            pass
                        else:
                            raise e

                if self._need_goto_next_file_cursor(
                        _datetime=_d, _size=_stat.st_size
                ):
                    self._current_start_date = _d
                    self._close()
                    self._current_start_date = _now()
                else:
                    self._current_start_date = _d

            except (
                    OSError, Exception, FileNotFoundError, PermissionError
            ):
                traceback.print_exc()

    def _create_new_logger_handler(self):
        """
        use to instantiate the logger use the config
        Returns:

        """

        # Create logger
        logger = self.get_logger(self.name)
        logger.setLevel(self._lvl)
        file_handler_formatter = logging.Formatter(
            self.file_log_format, "%Y-%m-%d %H:%M:%S"
        )

        already_installed_handlers = []
        for handler in logger.handlers:
            if hasattr(handler, "stream"):
                if handler.stream.name == "<custom-logger-io>":
                    logger.removeHandler(handler)
                    continue
                already_installed_handlers.append(
                    handler.stream.name.replace("\\", "/")
                )

        if self.log_capture_string and not any(
                [h == "<custom-logger-io>" for h in already_installed_handlers]
        ):
            ch = logging.StreamHandler(self.log_capture_string)
            ch.setFormatter(file_handler_formatter)
            logger.addHandler(ch)
        # Set FileHandler
        if self.base_file_name is not None and not any(
                (self.base_file_name + self.extension).replace("\\", "/") == h
                for h in already_installed_handlers
        ):
            log_file = self.base_file_name + self.extension
            self._set_current_start_date()

            try:
                open(log_file, "x").close()
            except FileExistsError:
                pass
            handler = logging.FileHandler(log_file)
            handler.setFormatter(file_handler_formatter)
            logger.addHandler(handler)

        elif self.base_file_name is not None and any(
                ((self.base_file_name or "")
                 + self.extension).replace("\\", "/") == h
                for h in already_installed_handlers
        ):
            self._set_current_start_date()

        if self.console:
            console_handler_formatter = logging.Formatter(
                self.console_log_format
            )
            if not any([h == "<stderr>" for h in already_installed_handlers]):
                console_handler_err = logging.StreamHandler(
                    sys.stderr
                )  # sys.stderr or sys.stdout
                console_handler_err.setFormatter(console_handler_formatter)

                logger.addHandler(console_handler_err)

        self.writer = logger

    @property
    def log_file(self):
        if self.base_file_name is None:
            return None
        try:
            self.writer.handlers[0].stream.truncate()
        except OSError:
            self._create_new_logger_handler()
        return self.writer.handlers[0].stream

    def _close(self):
        try:
            _file = self.base_file_name + self.extension
            base = os.path.splitext(_file)[0]
            last_date = datetime.datetime.fromtimestamp(
                os.stat(_file).st_mtime,
                tz=datetime.timezone.utc
            )
            new_file = get_no_filepath(
                base + "_"
                + self._current_start_date.strftime(
                    self._suffix_date_format
                )
                + "_to_"
                + last_date.strftime(self._suffix_date_format)
                + self.extension
            )

            shutil.copy(
                _file,
                new_file
            )

            with open(_file, "w"):
                pass

            self._set_seek()
        except:  # noqa E722
            pass

    def _set_seek(self):
        if self.writer:
            for handler in self.writer.handlers:
                if hasattr(handler, "stream"):
                    if os.path.isfile(handler.stream.name):
                        handler.stream.seek(0)

    def _log(self, msg, *args, level="INFO", **kwargs):
        """
        Use to log
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting
            level: str, set of (INFO|WARNING|EXCEPTION|CRITICAL|ERROR)

        Returns:

        """
        kwargs.pop("end", "\n")
        recreate = False
        try:
            self.writer.handlers[0].stream.truncate()
        except OSError:
            recreate = True

        last_file_size = self._size
        if self.base_file_name is not None:

            self._size = os.stat(self.base_file_name + self.extension).st_size
            if self._size < last_file_size:
                self._set_seek()
            elif recreate:
                # self._close()
                self._create_new_logger_handler()
            elif self._need_goto_next_file_cursor(_size=self._size):
                self._close()
                self._current_start_date = _now()
        try:
            msg = str(msg) % args
        except (TypeError, Exception):
            msg = " ".join([str(msg)] + [str(p) for p in args])
        getattr(self.writer, level.lower())(msg, **kwargs)
        if self.callback and self.callback_each_logging:
            self.send_all_logger_message_by_callback()

    def __call__(self, *args, **kwargs):
        self._log(*args, **kwargs)

    def info(self, msg="\n", *args, **kwargs):
        """
        log info
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting

        Returns:

        """
        self._log(msg, *args, level="INFO", **kwargs)

    def exception(self, msg="\n", *args, **kwargs):
        """
        log exception
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting

        Returns:

        """
        self._log(msg, *args, level="EXCEPTION", **kwargs)

    def critical(self, msg="\n", *args, **kwargs):
        """
        log critical
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting

        Returns:

        """
        self._log(msg, *args, level="CRITICAL", **kwargs)

    def error(self, msg="\n", *args, **kwargs):
        """
        log error
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting

        Returns:

        """
        self._log(msg, *args, level="ERROR", **kwargs)

    def warning(self, msg="\n", *args, **kwargs):
        """
        log warning
        Args:
            msg: str, the msg that you want to log
            *args: list|tuple, args for msg formatting

        Returns:

        """
        self._log(msg, *args, level="WARNING", **kwargs)
