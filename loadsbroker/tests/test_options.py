import json
import unittest
import requests

from mock import patch, Mock
from nose.tools import ok_, eq_, assert_raises

from loadsbroker.options import OptionLoader, OptionLoaderException

test_json = {"key": "value",
             "_overrides": {"override": "beta", "append": "gamma"}}
test_invalid = """{bogus"""

test_list = ["ENV_FOO=$key", "override=alpha", "ENV_BAR=$unknown"]
test_dict = {"FOO": "$key", "override": "alpha", "BAR": "$unknown"}


class TestOptions(unittest.TestCase):

    @patch("requests.get")
    def test_load_from_url(self, m_request):
        m_resp = Mock(spec=requests.Response)
        m_resp.content = json.dumps(test_json)
        m_request.return_value = m_resp
        res = OptionLoader.load_from_url("http://example.com")
        eq_(res, test_json)
        m_resp.content = test_invalid
        with assert_raises(OptionLoaderException) as ex:
            OptionLoader.load_from_url("http://example.com")
        ok_("Could not fetch new template" in ex.exception.args[0])

    # The positive of this test is already in the main flow.
    def test_invalid_load_from_file(self):
        with assert_raises(OptionLoaderException) as ex:
            OptionLoader.load_from_file("non_existent_file.foo")
        ok_("Could not open new template" in ex.exception.args[0])

    @patch("loadsbroker.options.OptionLoader.load_from_url")
    def test_update_from_url(self, m_loader):
        m_loader.return_value = test_json.copy()
        res = OptionLoader.update_from_url("ignored", test_list)
        eq_(res, [
            "ENV_FOO=value",
            "override=beta",
            "ENV_BAR=$unknown",
            "append=gamma"
            ])
        # sometimes a copy isn't.
        test_json['_overrides']['override'] = 'beta'
        m_loader.return_value = test_json.copy()
        res = OptionLoader.update_from_url("ignored", test_dict)
        eq_(res, {
            "BAR": "$unknown",
            "FOO": "value",
            "append": "gamma",
            "key": "value",
            "override": "beta"
        })

    @patch("loadsbroker.options.OptionLoader.load_from_file")
    def test_update_from_file(self, m_loader):
        m_loader.return_value = test_json.copy()
        res = OptionLoader.update_from_file("ignored", test_list)
        eq_(res, [
            "ENV_FOO=value",
            "override=beta",
            "ENV_BAR=$unknown",
            "append=gamma"
            ])
        # sometimes a copy isn't.
        test_json['_overrides']['override'] = 'beta'
        m_loader.return_value = test_json.copy()
        res = OptionLoader.update_from_file("ignored", test_dict)
        eq_(res, {
            "BAR": "$unknown",
            "FOO": "value",
            "append": "gamma",
            "key": "value",
            "override": "beta"
        })
