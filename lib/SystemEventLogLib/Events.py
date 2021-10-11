import threading
from datetime import datetime

from Cb_constants import CbServer
from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper


class Event(object):
    # Size in bytes
    max_size = 3072

    class Fields(object):
        """Holds all valid fields supported by the SystemEvent REST API"""
        UUID = "uuid"
        COMPONENT = "component"
        EVENT_ID = "event_id"
        SEVERITY = "severity"
        TIMESTAMP = "timestamp"
        DESCRIPTION = "description"
        NODE_NAME = "node"
        SUB_COMPONENT = "sub_component"
        EXTRA_ATTRS = "extra_attributes"

        @staticmethod
        def values(only_mandatory_fields=False):
            if only_mandatory_fields:
                return [Event.Fields.EVENT_ID, Event.Fields.UUID,
                        Event.Fields.COMPONENT, Event.Fields.TIMESTAMP,
                        Event.Fields.SEVERITY, Event.Fields.DESCRIPTION]
            return [var_name for var_name in Event.Fields.__dict__.keys()
                    if not var_name.startswith("__")]

    class Component(object):
        """Holds all valid components supported by the SystemEvent REST API"""
        NS_SERVER = "ns_server"
        QUERY = "query"
        INDEXING = "indexing"
        SEARCH = "search"
        EVENTING = "eventing"
        ANALYTICS = "analytics"
        BACKUP = "backup"
        XDCR = "xdcr"
        DATA = "data"
        SECURITY = "security"
        VIEWS = "views"

        @staticmethod
        def values():
            return [var_name for var_name in Event.Component.__dict__.keys()
                    if not var_name.startswith("__")]

    class Severity(object):
        """Holds all valid log levels supported by the SystemEvent REST API"""
        INFO = "info"
        ERROR = "error"
        WARN = "warn"
        FATAL = "fatal"

        @staticmethod
        def values():
            return [var_name for var_name in Event.Severity.__dict__.keys()
                    if not var_name.startswith("__")]


class EventHelper(object):
    class EventCounter(object):
        def __init__(self):
            self.__lock = threading.Lock()
            self.counter = 0

        def increment(self):
            with self.__lock:
                self.counter += 1
                return self.counter

        @property
        def max_events_reached(self):
            return self.counter > CbServer.max_sys_event_logs

    def __init__(self):
        # Saves the start time of test to help during validation
        self.test_start_time = None
        # Holds all events raised within the framework
        self.events = list()
        # Boolean to track whether we reached the max_event count to rollover
        self.__max_events_reached = False
        # Boolean to help tracking events which may occur in parallel
        self.__parallel_events = False
        # Counter to track total_number of events per run
        self.__event_counter = EventHelper.EventCounter()

    @staticmethod
    def get_timestamp_format(datetime_obj):
        """
        Returns a valid datetime string in UTC format
        as supported by SystemEventLogs"""
        ms_str = datetime_obj.strftime("%f")[:3]
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.") + ms_str + 'Z'

    @staticmethod
    def __duplicate_event_ids_present(events):
        """
        :param events: List of events to be validated
        :returns boolean: 'False' in case of failure due to duplicate UUID
        """
        uuids = set()
        for event in events:
            uuids.add(event[Event.Fields.UUID])
        return len(events) != len(uuids)

    def set_test_start_time(self):
        self.test_start_time = self.get_timestamp_format(datetime.now())

    def validate(self, server, since_time=None, events_count=None):
        """
        Validates the cluster events against the given events_list
        :param server: Server from which we can get the cluster events
        :param since_time: Start time from which the events should be fetched
        :param events_count: Number of events to be fetched. '-1' means all
        :return list: List containing failures
        """
        failures = list()
        rest = SystemEventRestHelper([server])
        # Fetch all events from the server for generic validation
        events = rest.get_events(server=server)

        # Check for event_id duplications
        if self.__duplicate_event_ids_present(events):
            failures.append("Duplicate event_ids seen")

        # Fetch events from the cluster and validate against the ones the
        # test has recorded
        v_index = 0
        events = rest.get_events(server=server, since_time=since_time,
                                 events_count=events_count)
        for event in events:
            if isinstance(self.events[v_index], list):
                # Process events which occurred in parallel from test's POV
                # TODO: Write a algo to find the match from the subset
                pass
            else:
                dict_to_compare = {k: event.get(k, None)
                                   for k in self.events[v_index].keys()}
                if dict_to_compare == event:
                    v_index += 1
                    if v_index == self.__event_counter.counter:
                        break
        else:
            failures.append("Unable to validate event: %s"
                            % self.events[v_index])
        return failures

    def enable_parallel_events(self):
        self.__parallel_events = True
        self.events.append(list())

    def disable_parallel_events(self):
        self.__parallel_events = False

    def add_event(self, event_dict):
        # No need to track events if we are not validating
        if self.test_start_time is None:
            return

        if self.__parallel_events:
            self.events[-1].append(event_dict)
        else:
            self.events.append(event_dict)

        # Increment the events counter
        self.__event_counter.increment()

        # Roll over old log if max_events have reached
        if self.__event_counter.max_events_reached:
            self.events.pop(0)
