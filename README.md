# testtest




import json
import inspect
from functools import partial
from ast import literal_eval
import requests
import numpy as np
from dateutil import parser
import pandas as pd
# Disable 'Unverified HTTPS request' warning.
requests.packages.urllib3.disable_warnings()

from ezbbg.ws import git_version


__author__ = ('eruiz070210', 'dgaraud111714')
LOCAL_BBG = False

HOST = 'localhost'
PORT = 6666
HEADERS = {'Content-type': 'application/json', 'Accept': 'text/plain'}
DEFAULT_TIMEOUT = 30
# ComputerID:PORT or IP_address:PORT only via HTTPS
URL_EZBBG_ROOT = "https://{0}:{1}"
URL_REFERENCE_DATA = '/'.join([URL_EZBBG_ROOT, "reference_data"])
URL_HISTORICAL_DATA = '/'.join([URL_EZBBG_ROOT, "historical_data"])
URL_BBG_VERSION = '/'.join([URL_EZBBG_ROOT, "version/bbg"])
URL_WS_VERSION = '/'.join([URL_EZBBG_ROOT, "version/ws"])
URL_FIELDS_INFO = '/'.join([URL_EZBBG_ROOT, "fields_info"])
URL_FIELDS = '/'.join([URL_EZBBG_ROOT, "fields"])
URL_FIELDS_BY_CATEGORY = '/'.join([URL_EZBBG_ROOT, "fields_by_category"])
URL_CHAIN_HIST = '/'.join([URL_EZBBG_ROOT, "chain_historical_data"])


def _refdata_converter(data):
    """Convert the deepest value of the JSON response to a DataFrame if it's
    possible.
    """
    for dictionary in data.itervalues():
        for key, unicode_str in dictionary.iteritems():
            if isinstance(unicode_str, basestring):
                try:
                    dictionary[key] = np.datetime64(parser.parse(dictionary[key]).date())
                except (ValueError, TypeError):
                    pass
                try:
                    if not isinstance(dictionary[key], np.datetime64):                    
                        dictionary[key] = pd.read_json(dictionary[key]).apply(
                            lambda x: pd.to_datetime(x) if x.dtypes == 'object' else x, axis=0)
                    else:
                        dictionary[key] = pd.to_datetime(dictionary[key])

                except ValueError:
                    pass
                try:
                    dictionary[key] = pd.DataFrame.from_dict(literal_eval(unicode_str))
                except:
                    pass
    return data


def _ezbbg_server_version(host, port):
    """Get the version of ezbbg which runs on the server.
    """
    response = requests.get(URL_BBG_VERSION.format(host, port),
                            headers=HEADERS,
                            verify=False)
    response.raise_for_status()
    return response.content

def _service_version(host, port):
    """Get the version of Web Service on server side.
    """
    response = requests.get(URL_WS_VERSION.format(host, port),
                            headers=HEADERS,
                            verify=False)
    response.raise_for_status()
    return response.content


def _get_reference_data(ticker_list, field_list, host, port, **kwargs):
    reference_data_request = {
        'ticker_list': [x for x in ticker_list],
        'field_list': field_list
    }
    timeout = kwargs.pop('timeout', DEFAULT_TIMEOUT)
    reference_data_request.update(kwargs)
    reference_data_request_js = json.dumps(reference_data_request)
    response = requests.get(URL_REFERENCE_DATA.format(host, port),
                            data=reference_data_request_js,
                            headers=HEADERS,
                            verify=False,
                            timeout=timeout)
    response.raise_for_status()
    return _refdata_converter(response.json())


def _get_historical_data(ticker_list, field_list, start_date, end_date,
                        host, port, **kwargs):
    historical_data_request = {
        'ticker_list': [x for x in ticker_list],
        'field_list': field_list,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat()}
    timeout = kwargs.pop('timeout', DEFAULT_TIMEOUT)
    historical_data_request.update(kwargs)
    historical_data_request_js = json.dumps(historical_data_request)
    response = requests.get(URL_HISTORICAL_DATA.format(host, port),
                            data=historical_data_request_js,
                            headers=HEADERS,
                            verify=False,
                            timeout=timeout)
    response.raise_for_status()
    if response.text == 'Error':
        return None
    data_dict_json = response.json()
    result = {k: pd.read_json(v) for k,v in data_dict_json.iteritems()}
    # Add the label 'date' to each index, as the returned DataFrame of the
    # original 'get_historical_data'.
    for k, df in result.iteritems():
        df.index.name = "date"
        df.sort_index(inplace=True)
    return result


def _get_fields_info(field_list, host, port, return_field_documentation=True, **kwargs):
    fields_info_request = {
        'field_list': field_list,
        'return_field_documentation': return_field_documentation
    }
    timeout = kwargs.pop('timeout', DEFAULT_TIMEOUT)
    fields_info_request.update(kwargs)
    fields_info_request_js = json.dumps(fields_info_request)
    response = requests.get(URL_FIELDS_INFO.format(host, port),
                            data=fields_info_request_js,
                            headers=HEADERS,
                            verify=False,
                            timeout=timeout)
    response.raise_for_status()
    return response.json()


def _search_fields(search_string,
                   host,
                   port,
                   return_field_documentation=True,
                   include_categories=None,
                   include_product_type=None,
                   include_field_type=None,
                   exclude_categories=None,
                   exclude_product_type=None,
                   exclude_field_type=None,
                   **kwargs):
    fields_request = {
        'search_string': search_string,
        'return_field_documentation': return_field_documentation,
        'include_categories': include_categories,
        'include_product_type': include_product_type,
        'include_field_type': include_field_type,
        'exclude_categories': exclude_categories,
        'exclude_product_type': exclude_product_type,
        'exclude_field_type': exclude_field_type
    }
    timeout = kwargs.pop('timeout', DEFAULT_TIMEOUT)
    fields_request.update(kwargs)
    fields_request_js = json.dumps(fields_request)
    response = requests.get(URL_FIELDS.format(host, port),
                            data=fields_request_js,
                            headers=HEADERS,
                            verify=False,
                            timeout=timeout)
    response.raise_for_status()
    return response.json()


def _search_fields_by_category(search_string,
                               host,
                               port,
                               return_field_documentation=True,
                               exclude_categories=None,
                               exclude_product_type=None,
                               exclude_field_type=None,
                               **kwargs):
    fields_by_category_request = {
        'search_string': search_string,
        'return_field_documentation': return_field_documentation,
        'exclude_categories': exclude_categories,
        'exclude_product_type': exclude_product_type,
        'exclude_field_type': exclude_field_type
    }
    timeout = kwargs.pop('timeout', DEFAULT_TIMEOUT)
    fields_by_category_request.update(kwargs)
    fields_by_category_request_js = json.dumps(fields_by_category_request)
    response = requests.get(URL_FIELDS_BY_CATEGORY.format(host, port),
                            data=fields_by_category_request_js,
                            headers=HEADERS,
                            verify=False,
                            timeout=timeout)
    response.raise_for_status()
    return response.json()


def _chain_historical_data(tickers, fields, end_date, host, port,
                           start_date=None, tolerance_in_days=4):
    if not start_date:
        start_date = (end_date - pd.DateOffset(years=5)).date()
    body = {"tickers": [x for x in tickers],
            'fields': [x for x in fields],
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'tolerance_days': tolerance_in_days}
    resp = requests.get(URL_CHAIN_HIST.format(host, port),
                        data=json.dumps(body), headers=HEADERS, verify=False)
    resp.raise_for_status()
    data_json = resp.json()
    # Load JSON for historical data
    hist_data = {k: pd.read_json(v) for k,v in data_json["data"].iteritems()}
    for k, df in hist_data.iteritems():
        df.index.name = "date"
    # Convert dates for the chaining info dict
    info = data_json["info"]
    for chain_info in info.values():
        for couple in chain_info:
            couple["chaining_start_date"] = pd.Timestamp(couple["chaining_start_date"])
    return {"info": info,
            'data': hist_data}


def update_host(host, port=PORT):
    """Update the (host, port) parameters for all HTTP client functions.
    """
    frame = inspect.currentframe()
    try:
        frame.f_globals["get_reference_data"] = partial(_get_reference_data,
                                                        host=host, port=port)
        frame.f_globals["get_historical_data"] = partial(_get_historical_data,
                                                         host=host, port=port)
        frame.f_globals["ezbbg_server_version"] = partial(_ezbbg_server_version,
                                                          host=host, port=port)
        frame.f_globals["service_version"] = partial(_service_version,
                                                     host=host, port=port)
        frame.f_globals["get_fields_info"] = partial(_get_fields_info,
                                                     host=host, port=port)
        frame.f_globals["search_fields"] = partial(_search_fields,
                                                   host=host, port=port)
        frame.f_globals["search_fields_by_category"] = partial(_search_fields_by_category,
                                                               host=host, port=port)
        frame.f_globals["get_and_chain_historical_data"] = partial(_chain_historical_data,
                                                                   host=host, port=port)
    finally:
        del frame


get_reference_data = partial(_get_reference_data, host=HOST, port=PORT)
get_historical_data = partial(_get_historical_data, host=HOST, port=PORT)
ezbbg_server_version = partial(_ezbbg_server_version, host=HOST, port=PORT)
service_version = partial(_service_version, host=HOST, port=PORT)
get_fields_info = partial(_get_fields_info, host=HOST, port=PORT)
search_fields = partial(_search_fields, host=HOST, port=PORT)
search_fields_by_category = partial(_search_fields_by_category, host=HOST, port=PORT)
get_and_chain_historical_data = partial(_chain_historical_data, host=HOST, port=PORT)


def check_versions():
    """Check the version between the client and the server.
    """
    remote = service_version()
    local = git_version()
    if local != remote:
        print "Web Service versions mismatch between the client and the server"
        print "Server: '{}'".format(remote)
        print "Client: '{}'".format(local)
        return False
    print "Server/client Versions OK"
    return True


if __name__ == "__main__":
    from datetime import date

    # host = 'FR09256841D'
    host = 'FR09263537D'
    #host = 'localhost'

    ticker_list = ['SX5E Index', 'SPX Index']
    field_list = ['PX_OPEN', 'PX_LAST']

    start, end = date(2012, 1, 1), date(2014, 1, 1)

    update_host(host)

    histo_data = get_historical_data(ticker_list, field_list, start, end)
    data = get_reference_data(ticker_list, field_list)

    fields_info = get_fields_info(field_list)

    search_string = 'earnings'

    fields_1 = search_fields(search_string, include_categories=["Ratings"])
    fields_2 = search_fields(search_string, include_categories=["Market"])

    fields_by_category = search_fields_by_category(search_string)



















ezbbg.ws. __init__.py
# -*- coding: utf-8 -*-

"""ezbbg Web Service based on Flask.

Run the server side with `python -m ezbbg.ws`
"""

import os.path as osp


__author__ = 'dgaraud111714'


def git_version():
    """Get the Web Service 'version'

    i.e. a representation of the latest Git commit.
    """
    wsdir = osp.dirname(__file__)
    with open(osp.join(wsdir, "version")) as fobj:
        return fobj.read().strip()
