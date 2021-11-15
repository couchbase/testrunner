import threading
import uuid
from datetime import datetime, timedelta

from Cb_constants import CbServer
from SystemEventLogLib.SystemEventOperations import SystemEventRestHelper


class Event(object):
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
            values = list()
            for var_name in Event.Fields.__dict__.keys():
                attr = getattr(Event.Fields, var_name)
                if var_name.startswith("__") or callable(attr):
                    continue
                values.append(attr)
            return values

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
            values = list()
            for var_name in Event.Component.__dict__.keys():
                attr = getattr(Event.Component, var_name)
                if var_name.startswith("__") or callable(attr):
                    continue
                values.append(attr)
            return values

    class Severity(object):
        """Holds all valid log levels supported by the SystemEvent REST API"""
        INFO = "info"
        ERROR = "error"
        WARN = "warn"
        FATAL = "fatal"

        @staticmethod
        def values():
            values = list()
            for var_name in Event.Severity.__dict__.keys():
                attr = getattr(Event.Severity, var_name)
                if var_name.startswith("__") or callable(attr):
                    continue
                values.append(attr)
            return values


class EventHelper(object):
    max_events = CbServer.sys_event_def_logs

    class EventCounter(object):
        def __init__(self):
            self.__lock = threading.Lock()
            self.counter = 0

        def set(self, new_value):
            with self.__lock:
                self.counter = new_value

        def increment(self):
            with self.__lock:
                self.counter += 1
            return self.counter

        @property
        def max_events_reached(self):
            return self.counter > EventHelper.max_events

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
    def get_rand_uuid():
        return str(uuid.uuid4())

    @staticmethod
    def get_timestamp_format(datetime_obj):
        """
        Returns a valid datetime string in UTC format
        as supported by SystemEventLogs"""
        ms_str = datetime_obj.strftime("%f")[:3]
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.") + ms_str + 'Z'

    @staticmethod
    def get_datetime_obj_from_str(datetime_str):
        return datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%fZ")

    @staticmethod
    def __duplicate_event_ids_present(events, failures):
        """
        :param events: List of events to be validated
        :param failures: List of failures detected
        :returns boolean: 'True' in case of failure due to duplicate UUID
        """
        duplicates_present = False
        timestamps = dict()
        prev_timestamp = None
        for event in events:
            event_uuid = event[Event.Fields.UUID]
            event_timestamp = event[Event.Fields.TIMESTAMP]

            # Check whether the events are ordered by timestamp in server
            curr_event_datetime = EventHelper.get_datetime_obj_from_str(
                event_timestamp)
            if prev_timestamp is not None:
                prev_event_timestamp = EventHelper.get_datetime_obj_from_str(
                    prev_timestamp)
                if curr_event_datetime < prev_event_timestamp:
                    duplicates_present = True
                    failures.append("Events not ordered by time !! "
                                    "UUID %s - timestamp (curr) %s < %s (prev)"
                                    % (event_uuid,
                                       event_timestamp, prev_timestamp))
            prev_timestamp = event_timestamp
            # End of timestamp validation

            # Check whether the events are not duplicated within desired time
            if event_uuid not in timestamps:
                timestamps[event_uuid] = event_timestamp
            else:
                ts_for_prev_uuid = EventHelper.get_datetime_obj_from_str(
                    timestamps[event_uuid])
                ts_for_curr_uuid = EventHelper.get_datetime_obj_from_str(
                    event_timestamp)
                if (ts_for_curr_uuid - ts_for_prev_uuid) \
                        > timedelta(seconds=CbServer.sys_event_log_uuid_uniqueness_time):
                    duplicates_present = True
                    failures.append("Duplicate UUID detected for timestamps!! "
                                    "%s is present during %s and %s"
                                    % (event_uuid, timestamps[event_uuid],
                                       event_timestamp))
            # End of duplicate UUID validation
        return duplicates_present

    @staticmethod
    def update_event_extra_attrs(event, extra_attr_dict):
        event[Event.Fields.EXTRA_ATTRS] = extra_attr_dict

    def _set_counter(self, counter):
        """
        Hard resets the event count within the EventHelper
        :param counter: New value for event count
        WARNING: Never use this unless truly required for testing purpose
        """
        self.__event_counter.set(counter)

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

        # If nothing stored in event list, nothing to validate
        if not self.events:
            return failures

        rest = SystemEventRestHelper([server])
        # Fetch all events from the server for generic validation
        events = rest.get_events(server=server)

        # Check for event_id duplications
        if self.__duplicate_event_ids_present(events, failures):
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
