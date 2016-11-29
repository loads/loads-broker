"""Convenience objects for broker options"""
import json
import requests

from string import Template
from collections import namedtuple


class HekaOptions(namedtuple("HekaOptions", "host port secure")):
    """Named tuple containing Heka options."""


class InfluxOptions(namedtuple("InfluxOptions",
                               "host port user password secure")):
    """Named tuple containing InfluxDB options."""


class OptionLoaderException(Exception):
    pass


class OptionLoader:

    @classmethod
    def load_from_url(cls, url):
        """Load JSON data from a remote URL

        """
        try:
            req = requests.get(url, timeout=2)
            template = req.content
            req.close()
            return json.loads(template)
        except (requests.HTTPError, ValueError) as ex:
            raise OptionLoaderException(
                "Could not fetch new template: {}".format(ex)
            )

    @classmethod
    def load_from_file(cls, filepath):
        """Load JSON data from a local file

        """
        try:
            tfile = open(filepath)
            return json.loads(tfile.read())
        except (IOError, ValueError) as ex:
            raise OptionLoaderException(
                "Could not open new template: {}".format(ex)
            )

    @classmethod
    def update_from_url(cls, url, params, **kwargs):
        """Update a parameter set from a remote URL

        """
        kwargs.update(cls.load_from_url(url))
        if isinstance(params, list):
            return cls.update_list(params, **kwargs)
        if isinstance(params, dict):
            return cls.update_dict(params, **kwargs)

    @classmethod
    def update_from_file(cls, filepath, params, **kwargs):
        """Update a parameter set from a local file

        """
        kwargs.update(cls.load_from_file(filepath))
        if isinstance(params, list):
            return cls.update_list(params, **kwargs)
        if isinstance(params, dict):
            return cls.update_dict(params, **kwargs)

    @classmethod
    def update_dict(cls, param_dict, **kwargs):
        """"Update a dict replacing interpoles in values

        The dictionary will then be updated with all keys.

        See `update_list` for use and examples.

        :param param_dict: dictionary of values to interpolate
        :type param_dict: dict
        :param kwargs:
        :type kwargs: dict

        """
        overrides = kwargs.pop("_overrides", {})
        if overrides:
            kwargs.update(overrides)
        if kwargs:
            for key in param_dict:
                param_dict[key] = Template(
                    param_dict[key]).safe_substitute(kwargs)
            param_dict.update(kwargs)
        return param_dict

    @classmethod
    def update_list(cls, param_list, **kwargs):
        """Update a list of key=value pairs replacing interpoles.

        The replacement json is a set of variables and definitions, with a
        special value called "_overrides". The "_overrides" will either
        override a matching key in the parameter list or will be appended
        to the end of the parameter list.

        for instance, if the original parameter list contains:

        [
            "alpha=apple",
            "beta=$berry",
            "gamma=grape",
            "delta="
         ]

        and the kwargs contains:
        {
            "berry": "$fruit",
            "fruit": "banana"
            "_overrides": {
                "gamma": "gorgonzola",
                "epsilon": "egg"
            }
        }

        Then the resulting list would read:
        [
            "alpha=apple",
            "beta=banana",
            "gamma=gorgonzola",
            "delta=",
            "epsilon=egg"

        ]

        :param param_list: dictionary of parameters to modify
        :type param_list: list
        :param kwargs: replacement parameters and _overrides
        :type kwargs: dict
        :rtype list
        """
        overrides = kwargs.pop("_overrides", {})
        new_list = []
        # iterate over source replacing vars until done.
        for item in param_list:
            item_name = item.split('=')[0]
            if item_name in overrides:
                val = overrides[item_name]
                item = "{}={}".format(item_name, "" if not val else val)
                del(overrides[item_name])
            pre = ""
            while item != pre:
                pre = item
                item = Template(pre).safe_substitute(kwargs)
            new_list.append(item)
        if overrides:
            for key in overrides:
                val = overrides[key]
                new_list.append("{}={}".format(key,
                                               "" if not val else val))
        return new_list
