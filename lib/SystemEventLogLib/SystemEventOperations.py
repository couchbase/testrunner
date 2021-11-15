import json
import urllib.parse

import requests
from copy import deepcopy
from random import choice

from Cb_constants import CbServer
from lib.membase.api.rest_client import RestConnection


class SystemEventRestHelper:
    def __init__(self, server_list):
        """
        :param server_list: Valid list of servers through which event
                            specific APIs can be accessed"""
        self.servers = server_list

    def get_rest_object(self, rest, server, username, password):
        def update_auth(r_obj, u_name, p_word):
            if u_name is not None:
                r_obj = deepcopy(r_obj)
                r_obj.username = u_name
                r_obj.password = p_word
            return r_obj
        if rest is not None:
            return update_auth(rest, username, password)

        if server:
            rest = RestConnection(server)
        else:
            rest = RestConnection(choice(self.servers))
        rest = update_auth(rest, username, password)
        return rest

    def create_event(self, event_dict, rest=None, server=None,
                     username=None, password=None):
        """
        Create event in the cluster as per event_dict
        :param event_dict: Dictionary containing parameters for an event
        :param rest: RestConnection object to send requests
        :param server: Target server to create RestConnection
        :param username: Username auth to use during API operations
        :param password: Password auth to use during API operations
        """
        rest = self.get_rest_object(rest, server, username, password)
        api = rest.baseUrl + "_event"
        req_data = '{'
        for key, value in event_dict.items():
            kv_format = '"%s":'
            if isinstance(value, int):
                kv_format += '%s,'
            else:
                kv_format += '"%s",'
            req_data += kv_format % (key, value)
        req_data = req_data[:-1] + '}'
        status, content, _ = rest._http_request(
            api, method=RestConnection.POST, params=req_data,
            headers=rest.get_headers_for_content_type_json())
        return status, json.loads(content)

    def create_event_stream(self, rest=None, server=None,
                            username=None, password=None):
        """
        Creates an event stream object to the specific cluster node
        :param rest: RestConnection object to send requests
        :param server: Target server to create RestConnection
        :param username: Username auth to use during API operations
        :param password: Password auth to use during API operations
        """
        rest = self.get_rest_object(rest, server, username, password)
        api = rest.baseUrl + "eventsStreaming"
        status, content, _ = rest._http_request(api, method=RestConnection.GET)
        return json.loads(content)

    def get_events(self, rest=None, server=None, username=None, password=None,
                   since_time=None, events_count=None):
        """
        Fetches events from the cluster_node with respect to
        optional since_time and event_count
        :param rest: RestConnection object to send requests
        :param server: Target server to create RestConnection
        :param username: Username auth to use during API operations
        :param password: Password auth to use during API operations
        :param since_time: Time from which the events needs to be fetched
        :param events_count: Number of events to fetch from the specific
                           'since_time' value"""
        rest = self.get_rest_object(rest, server, username, password)
        api = rest.baseUrl + "events"
        get_params = dict()
        if since_time:
            get_params.update({"sinceTime": since_time})
        if events_count is not None:
            get_params.update({"limit": events_count})

        response = requests.get(api, params=get_params,
                                auth=(rest.username,
                                      rest.password)).content
        return json.loads(response)["events"]

    def update_max_events(self,
                          max_event_count=CbServer.sys_event_def_logs,
                          rest=None, server=None,
                          username=None, password=None):
        """
        Update the maximum number of events that will be stored in the cluster
        :param max_event_count: New event count to be updated in the cluster
        :param rest: RestConnection object to send requests
        :param server: Target server to create RestConnection
        :param username: Username auth to use during API operations
        :param password: Password auth to use during API operations
        :return: status, content
        """
        rest = self.get_rest_object(rest, server, username, password)
        api = rest.baseUrl + "internalSettings"
        params = urllib.parse.urlencode({"maxEvents": max_event_count})
        status, content, _ = rest._http_request(api, RestConnection.POST,
                                                params=params)
        return status, json.loads(content)
