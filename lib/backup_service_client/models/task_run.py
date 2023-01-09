# coding: utf-8

"""
    Couchbase Backup Service API

    This is REST API allows users to remotely schedule and run backups, restores and merges as well as to explore various archives for all there Couchbase Clusters.  # noqa: E501

    OpenAPI spec version: 0.1.0
    
    Generated by: https://github.com/swagger-api/swagger-codegen.git
"""

import pprint
import re  # noqa: F401

import six


class TaskRun(object):
    """NOTE: This class is auto generated by the swagger code generator program.

    Do not edit the class manually.
    """
    """
    Attributes:
      swagger_types (dict): The key is attribute name
                            and the value is attribute type.
      attribute_map (dict): The key is attribute name
                            and the value is json key in definition.
    """
    swagger_types = {
        'task_name': 'str',
        'status': 'str',
        'start': 'str',
        'end': 'str',
        'node_runs': 'list[TaskRunNodeRuns]',
        'error': 'str',
        'error_code': 'int',
        'type': 'str',
        'target_cluster': 'str'
    }

    attribute_map = {
        'task_name': 'task_name',
        'status': 'status',
        'start': 'start',
        'end': 'end',
        'node_runs': 'node_runs',
        'error': 'error',
        'error_code': 'error_code',
        'type': 'type',
        'target_cluster': 'target_cluster'
    }

    def __init__(self, task_name=None, status=None, start=None, end=None, node_runs=None, error=None, error_code=None, type=None, target_cluster=None):  # noqa: E501
        """TaskRun - a model defined in Swagger"""  # noqa: E501
        self._task_name = None
        self._status = None
        self._start = None
        self._end = None
        self._node_runs = None
        self._error = None
        self._error_code = None
        self._type = None
        self._target_cluster = None
        self.discriminator = None
        if task_name is not None:
            self.task_name = task_name
        if status is not None:
            self.status = status
        if start is not None:
            self.start = start
        if end is not None:
            self.end = end
        if node_runs is not None:
            self.node_runs = node_runs
        if error is not None:
            self.error = error
        if error_code is not None:
            self.error_code = error_code
        if type is not None:
            self.type = type
        if target_cluster is not None:
            self.target_cluster = target_cluster

    @property
    def backup(self):
        if self.node_runs:
            for node_run in self.node_runs:
                if node_run.backup:
                    return node_run.backup
        return None

    @property
    def task_name(self):
        """Gets the task_name of this TaskRun.  # noqa: E501


        :return: The task_name of this TaskRun.  # noqa: E501
        :rtype: str
        """
        return self._task_name

    @task_name.setter
    def task_name(self, task_name):
        """Sets the task_name of this TaskRun.


        :param task_name: The task_name of this TaskRun.  # noqa: E501
        :type: str
        """

        self._task_name = task_name

    @property
    def status(self):
        """Gets the status of this TaskRun.  # noqa: E501


        :return: The status of this TaskRun.  # noqa: E501
        :rtype: str
        """
        return self._status

    @status.setter
    def status(self, status):
        """Sets the status of this TaskRun.


        :param status: The status of this TaskRun.  # noqa: E501
        :type: str
        """
        allowed_values = ["unknown", "failed", "done", "running", "waiting"]  # noqa: E501
        if status not in allowed_values:
            raise ValueError(
                "Invalid value for `status` ({0}), must be one of {1}"  # noqa: E501
                .format(status, allowed_values)
            )

        self._status = status

    @property
    def start(self):
        """Gets the start of this TaskRun.  # noqa: E501

        The start time for the task.  # noqa: E501

        :return: The start of this TaskRun.  # noqa: E501
        :rtype: str
        """
        return self._start

    @start.setter
    def start(self, start):
        """Sets the start of this TaskRun.

        The start time for the task.  # noqa: E501

        :param start: The start of this TaskRun.  # noqa: E501
        :type: str
        """

        self._start = start

    @property
    def end(self):
        """Gets the end of this TaskRun.  # noqa: E501

        The end time for the task  # noqa: E501

        :return: The end of this TaskRun.  # noqa: E501
        :rtype: str
        """
        return self._end

    @end.setter
    def end(self, end):
        """Sets the end of this TaskRun.

        The end time for the task  # noqa: E501

        :param end: The end of this TaskRun.  # noqa: E501
        :type: str
        """

        self._end = end

    @property
    def node_runs(self):
        """Gets the node_runs of this TaskRun.  # noqa: E501


        :return: The node_runs of this TaskRun.  # noqa: E501
        :rtype: list[TaskRunNodeRuns]
        """
        return self._node_runs

    @node_runs.setter
    def node_runs(self, node_runs):
        """Sets the node_runs of this TaskRun.


        :param node_runs: The node_runs of this TaskRun.  # noqa: E501
        :type: list[TaskRunNodeRuns]
        """

        self._node_runs = node_runs

    @property
    def error(self):
        """Gets the error of this TaskRun.  # noqa: E501

        Any errors that occur during the run will be displayed here.  # noqa: E501

        :return: The error of this TaskRun.  # noqa: E501
        :rtype: str
        """
        return self._error

    @error.setter
    def error(self, error):
        """Sets the error of this TaskRun.

        Any errors that occur during the run will be displayed here.  # noqa: E501

        :param error: The error of this TaskRun.  # noqa: E501
        :type: str
        """

        self._error = error

    @property
    def error_code(self):
        """Gets the error_code of this TaskRun.  # noqa: E501

        If there is an error there will be a non-zero error code  # noqa: E501

        :return: The error_code of this TaskRun.  # noqa: E501
        :rtype: int
        """
        return self._error_code

    @error_code.setter
    def error_code(self, error_code):
        """Sets the error_code of this TaskRun.

        If there is an error there will be a non-zero error code  # noqa: E501

        :param error_code: The error_code of this TaskRun.  # noqa: E501
        :type: int
        """

        self._error_code = error_code

    @property
    def type(self):
        """Gets the type of this TaskRun.  # noqa: E501

        Task type  # noqa: E501

        :return: The type of this TaskRun.  # noqa: E501
        :rtype: str
        """
        return self._type

    @type.setter
    def type(self, type):
        """Sets the type of this TaskRun.

        Task type  # noqa: E501

        :param type: The type of this TaskRun.  # noqa: E501
        :type: str
        """
        allowed_values = ["BACKUP", "MERGE", "RESTORE"]  # noqa: E501
        if type not in allowed_values:
            raise ValueError(
                "Invalid value for `type` ({0}), must be one of {1}"  # noqa: E501
                .format(type, allowed_values)
            )

        self._type = type

    @property
    def target_cluster(self):
        """Gets the target_cluster of this TaskRun.  # noqa: E501

        If the task is a restore it will have the target cluster  # noqa: E501

        :return: The target_cluster of this TaskRun.  # noqa: E501
        :rtype: str
        """
        return self._target_cluster

    @target_cluster.setter
    def target_cluster(self, target_cluster):
        """Sets the target_cluster of this TaskRun.

        If the task is a restore it will have the target cluster  # noqa: E501

        :param target_cluster: The target_cluster of this TaskRun.  # noqa: E501
        :type: str
        """

        self._target_cluster = target_cluster

    def to_dict(self):
        """Returns the model properties as a dict"""
        result = {}

        for attr, _ in six.iteritems(self.swagger_types):
            value = getattr(self, attr)
            if isinstance(value, list):
                result[attr] = list(map(
                    lambda x: x.to_dict() if hasattr(x, "to_dict") else x,
                    value
                ))
            elif hasattr(value, "to_dict"):
                result[attr] = value.to_dict()
            elif isinstance(value, dict):
                result[attr] = dict(map(
                    lambda item: (item[0], item[1].to_dict())
                    if hasattr(item[1], "to_dict") else item,
                    value.items()
                ))
            else:
                result[attr] = value
        if issubclass(TaskRun, dict):
            for key, value in self.items():
                result[key] = value

        return result

    def to_str(self):
        """Returns the string representation of the model"""
        return pprint.pformat(self.to_dict())

    def __repr__(self):
        """For `print` and `pprint`"""
        return self.to_str()

    def __eq__(self, other):
        """Returns true if both objects are equal"""
        if not isinstance(other, TaskRun):
            return False

        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Returns true if both objects are not equal"""
        return not self == other