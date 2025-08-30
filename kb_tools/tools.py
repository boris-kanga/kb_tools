# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import keyword
import os
import random
import re
import shutil
import string
import sys
import time
import traceback
import unicodedata

try:
    import Levenshtein as Lev
except ImportError:
    Lev = type(
        "Levenshtein",
        (),
        {"distance": lambda x, y: 0, "ratio": lambda x, y: 0},
    )()


INFINITE = 1.8e308


def apply_func(func, value, default=None):
    try:
        return func(value)
    except (KeyboardInterrupt, SyntaxError, SystemError):
        raise
    except:  # noqa: E722
        return default


def generate_password(size=None, punctuation=True):
    if size is None:
        size = int(os.environ.get("PASSWORD_SIZE", 8))
    part = random.sample(string.digits, int(size / 4))
    part += random.sample(string.ascii_letters, int(size / 2))
    if punctuation:
        part += random.sample(string.punctuation, int(size / 4))
    part += string.ascii_letters
    part = part[:size]
    random.shuffle(part)
    return "".join(part[:size])


def get_no_filepath(filepath):
    index = 1
    f, ext = os.path.splitext(filepath)
    while os.path.exists(filepath):
        index += 1
        filepath = f + "_" + str(index) + ext
    return filepath


def read_json_file(path, default=None) -> dict | list:
    """
    Use to get content of json file
    Args:
        path: str, the path of json file
        default: default

    Returns:
        json object (list|dict)
    """
    try:
        with open(path, encoding="utf-8") as json_file:
            param = json.load(json_file)
            return param
    except:  # noqa: E722
        return default


def get_func_args(func):
    while hasattr(func, "__wrapped__"):
        func = func.__wrapped__
    return func.__code__.co_varnames[: func.__code__.co_argcount]


def generate_candidate(a, *args):
    def _eq(self, o):
        for index, i in enumerate((a, *args)):
            if "%" in i:
                reg = "^" + re.escape(i).replace("%", ".*?") + "$"
                if re.match(reg, o, flags=re.I):
                    self.last_index = index
                    return True
            else:
                if Var(o) == i:
                    self.last_index = index
                    return True
        return False

    obj = type(
        "candidate",
        (),
        {
            "last_index": len((a, *args)),
            "__eq__": _eq,
            "__neq__": lambda self, o: not self.__eq__(o),
            "__str__": lambda s: a
        },
    )
    return obj()


def rename_file(path_to_last_file, new_name, *, use_origin_folder=False):
    """
    Use to rename or move file
    Args:
        path_to_last_file: the path to the file to be rename or move
        new_name: str, the new path | name
        use_origin_folder: bool, specify if the directory of new_name must be
            use for moving the file

    Returns:
        str, the path to the file renamed or moved

    """
    if os.path.exists(path_to_last_file):
        last_folder = os.path.dirname(path_to_last_file)
        if use_origin_folder:
            new_name = os.path.join(last_folder, os.path.basename(new_name))
        try:
            os.makedirs(os.path.dirname(new_name), exist_ok=True)
        except (OSError, Exception):
            pass
        shutil.move(path_to_last_file, new_name)
        return new_name


def remove_accent_from_text(text):
    """
    Strip accents from input String.

    text: The input string.

    returns:
        The processed String.

    """
    text = text.encode("utf-8").decode("utf-8")

    text = unicodedata.normalize("NFD", text)
    text = text.encode("ascii", "ignore")
    text = text.decode("utf-8")
    return str(text)


def format_var_name(
    name,
    sep="_",
    accent=False,
    permit_char=None,
    default="var",
    remove_accent=False,
    min_length_word=1,
    no_case=False,
    blacklist=keyword.kwlist,
):
    if re.match(r"^\d*$", str(name)):
        return str(default)
    origin = str(name).strip()
    name = origin

    # camel name --> camelName
    name = re.sub("(?<=[a-z])([A-Z])", r"_\1", name)
    if name != origin:
        no_case = True

    if no_case:
        name = name.lower()
    if accent:
        reg = r"\w\d_"
    else:
        if remove_accent:
            try:
                name = remove_accent_from_text(name)
            except (ValueError, Exception):
                pass
        reg = r"a-zA-Z\d"
    reg += re.escape("".join(permit_char or ""))
    reg = "[^" + reg + "]"
    name = sep.join(
        [
            p
            for p in re.sub(reg, " ", name, flags=re.I).strip().split()
            if p and len(p) >= min_length_word
        ]
    )
    name = sep.join([x for x in re.split(r"^(\d+)", name)[::-1] if x])

    if blacklist:
        if no_case:
            blacklist = [str(d).lower() for d in blacklist]
        while name in blacklist:
            name += "_"
    return name


def lev_calculate(str1, str2):
    dist = Lev.distance(str1, str2)
    r = Lev.ratio(str1, str2)
    return dist, r


class Var(str):
    def __new__(cls, *args, **kwargs):
        default = kwargs.pop("default", None)

        remove_accent = kwargs.pop("remove_accent", True)
        ratio = kwargs.pop("eq_ratio", 0.8)
        dist = kwargs.pop("eq_dist", 1)
        force = kwargs.pop("force", False)
        no_case = kwargs.pop("no_case", True)

        self = str.__new__(cls, *args, **kwargs)

        self._force = force
        self._ratio = ratio
        self._dist = dist
        self._remove_accent = remove_accent
        self._no_case = no_case

        # self = cls(*args, **kwargs)
        self._good = format_var_name(
            self, default=default or self, remove_accent=remove_accent
        ).replace("_", "")

        if self._no_case:
            self._good = self._good.lower()
        return self

    def __eq__(self, other):
        try:
            if super().__eq__(other):
                return True
            if not isinstance(other, Var):
                other = format_var_name(
                    other, default=other, remove_accent=self._remove_accent
                ).replace("_", "")
                if self._no_case:
                    other = other.lower()
        except AttributeError:
            return self == other
        res = self._good == other
        if res or not self._force:
            return res
        dist, ratio = lev_calculate(self._good, other)
        return dist >= (self._dist or 0) and ratio >= self._ratio

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return super().__hash__()


class Cdict(dict):
    NO_CAST_CONSIDER = True

    def __new__(cls, *args, **kwargs):
        data = {}
        if len(args) == 1:
            data = args[0]
        elif len(args) > 1:
            data = args
        __no_parse_string = kwargs.get("_Cdict__no_parse_string", False)
        __callback = kwargs.get("_Cdict__alter_callback", None)
        if isinstance(data, str) and not __no_parse_string:
            got = False
            try:
                if os.path.exists(data):
                    data = read_json_file(data, {})
                    got = True
            except (FileNotFoundError, FileExistsError, Exception):
                pass
            if not got:
                try:
                    data = json.loads(data)
                except json.decoder.JSONDecodeError:
                    return data
            if isinstance(data, list):
                return [
                    cls(
                        d,
                        _Cdict__no_parse_string=True,
                        _Cdict__alter_callback=__callback,
                    )
                    for d in data
                ]
            else:
                return cls(data, **kwargs)
        elif data is None:
            return None
        elif isinstance(data, str):
            return data
        elif isinstance(data, (int, float)):
            return data
        elif isinstance(data, (list, tuple, set)):
            return type(data)(
                [
                    cls(
                        d,
                        _Cdict__no_parse_string=True,
                        _Cdict__alter_callback=__callback,
                    )
                    for d in data
                ]
            )
        elif isinstance(data, dict):
            return dict.__new__(cls)
        return data

    def __init__(self, *args, **kwargs):

        kwargs.pop("_Cdict__no_parse_string", None)
        self.__callback = kwargs.pop("_Cdict__alter_callback", None)
        data = {}
        if len(args) == 1:
            data = args[0]
        elif len(args) > 1:
            data = args
        self.__file_name = None
        if isinstance(data, dict):
            data.update(kwargs)
        else:
            data = kwargs

        for k in data:
            data[k] = Cdict(
                data[k],
                _Cdict__no_parse_string=True,
                _Cdict__alter_callback=self.__callback,
            )
            # if isinstance(data[k], dict):
            #    data[k] = Cdict(data[k])

        super().__init__(data)

    def __callback_func(self):
        if callable(self.__callback):
            try:
                self.__callback()
            except:  # noqa: E722
                pass

    def __parse_item(self, item):
        for i in self.keys():
            if self.NO_CAST_CONSIDER:
                if Var(i) == item:
                    return i
            elif i == item:
                return i
        return item

    def to_json(self, file_path=None, indent=4, retrieve=False):
        result = None
        if retrieve:
            result = json.dumps(self, indent=indent)
        if file_path or self.__file_name:
            self._to_json(self, file_path or self.__file_name, indent=indent)
        return result

    @staticmethod
    def _to_json(json_data, file_path, indent=4):
        res = json.dumps(json_data, indent=indent)
        with open(file_path, "w") as file:
            file.write(res)

    def get(self, item, default=None):
        return getattr(self, item, default)

    def __getitem__(self, item):
        try:
            return super().__getitem__(item)
        except KeyError:
            key = self.__parse_item(item)
            return super().__getitem__(key)

    def __getattr__(self, item, *args):
        try:
            return self[item]
        except KeyError:
            if len(args):
                if len(args) == 1:
                    return args[0]
                return args
            raise AttributeError(
                "This attribute %s don't exists for this instance" % item
            )

    def pop(self, k, *args):
        k = self.__parse_item(k)
        res = super().pop(k, *args)
        self.__callback_func()
        return res

    def __contains__(self, item):
        return item in [
            Var(k) if self.NO_CAST_CONSIDER else k for k in self.keys()
        ] or super().__contains__(item)

    def __delitem__(self, k):
        """Delete self[key]."""
        k = self.__parse_item(k)
        super().__delitem__(k)
        self.__callback_func()

    def __setitem__(self, k, v):
        """Set self[key] to value."""
        k = self.__parse_item(k)
        v = Cdict(
            v,
            _Cdict__no_parse_string=True,
            _Cdict__alter_callback=self.__callback,
        )
        super().__setitem__(k, v)
        self.__callback_func()

    def __setattr__(self, key, value):
        if str(key).startswith("_Cdict__"):
            super().__setattr__(key, value)
            return
        value = Cdict(
            value,
            _Cdict__no_parse_string=True,
            _Cdict__alter_callback=self.__callback,
        )
        self.__setitem__(key, value)
        self.__callback_func()


def get_buffer(obj, max_buffer=200, vv=True) -> tuple | ...:
    i = 0
    if hasattr(obj, "shape"):

        def length(x):
            return x.shape[0]  # noqa E731

        size = max(int(obj.shape[0] / max_buffer), 1)
    else:
        length = len
        size = max(int(len(obj) / max_buffer), 1)
    for i in range(size):
        tmp = obj[i * max_buffer : (i + 1) * max_buffer]
        if length(tmp) > max_buffer:
            tmp = tmp[:-1]
        if vv:
            yield i / size, tmp
        else:
            yield tmp
    res = obj[(i + 1) * max_buffer :]
    if not length(res):
        return
    if vv:
        yield (i + 1) / size, res
    else:
        yield res


def is_file_is_used(file):
    if sys.platform != "linux":
        try:
            os.rename(file, file)
            return False
        except (OSError, Exception):
            return True
    try:
        file = os.path.realpath(file)
    except FileNotFoundError:
        return False

    import glob

    for fds in glob.glob("/proc/*/fd/*"):
        try:
            if os.readlink(fds) == file:
                return True
        except OSError:
            pass
    if os.path.splitext(file)[1].lower() in (".csv", ".txt", ""):
        try:
            with open(file, encoding="Latin") as f:
                nb_line = sum(1 for _ in f)
            time.sleep(0.1)
            with open(file, encoding="Latin") as f:
                return not (nb_line == sum(1 for _ in f))
        except (OSError, Exception):
            return True
    return False


def got_error(func):

    def inner(*args, **kwargs) -> bool:
        try:
            func(*args, **kwargs)
            return True
        except:  # noqa: E722
            traceback.print_exc()
            return False

    return inner


def colored_text(text, color=None):
    if isinstance(color, (tuple, list)):
        color = tuple(color[:3])
        return ("\033[38;2;%s;%s;%sm" % color) + str(text) + "\033[0m"
    return text


def last_file_lines(fname, N):
    # assert statement check
    # a condition
    assert N >= 0

    # declaring variable
    # to implement
    # exponential search
    pos = N + 1

    # list to store
    # last N lines
    lines = []

    # opening file using with() method
    # so that file get closed
    # after completing work
    with open(fname) as f:

        # loop which runs
        # until size of list
        # becomes equal to N
        while len(lines) <= N:

            # try block
            try:
                # moving cursor from
                # left side to
                # pos line from end
                f.seek(-pos, 2)

            # exception block
            # to handle any run
            # time error
            except IOError:
                f.seek(0)
                break

            # finally block
            # to add lines
            # to list after
            # each iteration
            finally:
                lines = list(f)

            # increasing value
            # of variable
            # exponentially
            pos *= 2

    # returning the
    # whole list
    # which stores last
    # N lines
    return lines[-N:]


def is_phone_number(number, retrieve=True, force_plus=True):
    number = re.sub(r"\s-_", "", str(number))

    if re.match(r"^\d{10}$", number):
        if number[:2] in ("01", "07", "05"):
            return True if not retrieve else "+225" + number
        return False if not retrieve else None

    res = re.match(
        r"^((?:00|\+)?[(\[]?(?:00|\+)?\d{1,3}[)\[]?)(\d{8,10})$", number
    )
    if not res:
        return False if not retrieve else None
    suffix, number = res.groups()
    suffix = (
        suffix.replace("(", "")
        .replace("[", "")
        .replace(")", "")
        .replace("]", "")
    )
    if force_plus:
        if not suffix.startswith(("00", "+")):
            return False if not retrieve else None
    try:
        suffix = int(re.sub("^00", "+", suffix))
        suffix = "+" + str(suffix)
    except ValueError:
        return False if not retrieve else None

    return True if not retrieve else suffix + number


if __name__ == "__main__":
    test = generate_candidate("%date%pass")
    print(test == "date_de_modificaion_mot_de_pass")
