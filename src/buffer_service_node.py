#!/usr/bin/python
"""
Defines a buffer service that can record any ROS topic (as a subscriber) and can then
replay those messages as JSON
"""

import json
import rospy
import rosgraph
from roslib import message as roslib_message
from ros_buffer_service.srv import *
import time
from rosbridge_library.internal import message_conversion

TIME_BUFFER = 1
COUNT_BUFFER = 2

class BufferObject:
    def __init__(self, msg):
        self.msg = msg
        self.timestamp = time.time()


class Buffer:
    """
    A FIFO buffer that is able to maintain a certain number of elements.
    """
    def __init__(self, buffer_size, buffer_size_type):
        self._buffer = list()
        self.buffer_size = buffer_size
        if buffer_size_type == 'time':
            self.buffer_type = TIME_BUFFER
        else:
            self.buffer_type = COUNT_BUFFER

    def add(self, data):
        """
        Add an element to the buffer. If the buffer exceeds the given size,
        then we pop the first element.
        :param data: a generic ROS MSG
        """
        if self.buffer_type == COUNT_BUFFER:
            if len(self._buffer) == self.buffer_size:
                self._buffer.pop(0)
        elif self.buffer_type == TIME_BUFFER:
            if len(self._buffer) > 0:
                if (self._buffer[-1].timestamp - self._buffer[0].timestamp) > self.buffer_size:
                    # If we are here, the number of seconds elapsed since the first message has exceeded buffer size
                    self._buffer.pop(0)
        self._buffer.append(BufferObject(data))

    def check_range(self, start_epoch, end_epoch):
        """
        Check if the current buffer has elements that are in the time-range requested
        :param start_epoch: epoch of the starting timestamp in the range
        :param end_epoch: epoch of the final timestamp in the range
        :return: bool
        """
        if start_epoch <= 0 or end_epoch <= 0 or start_epoch > end_epoch:
            return False
        if len(self._buffer) == 0:
            return False
        return self._buffer[0].timestamp <= start_epoch and self._buffer[-1].timestamp >= end_epoch

    def closest_timestamp_index(self, timestamp):
        return self.search_closest(self._buffer, 0, len(self._buffer), timestamp)

    @staticmethod
    def search_closest(array, low, high, timestamp):
        """
        Performs a binary search to the closest timestamp in the buffer
        :param array: a list of BufferNode objects
        :param low: index (0 -> len(buffer))
        :param high: index (0 -> len(buffer))
        :param timestamp: epoch time we are searching for
        :return: index of closest timestamp in the buffer
        """
        if low >= high:
            return low
        mid = (low + high) / 2
        if array[mid].timestamp == timestamp:
            return mid
        if array[mid].timestamp > timestamp:
            return Buffer.search_closest(array, low, mid, timestamp)
        return Buffer.search_closest(array, mid + 1, high, timestamp)

    def buffer_range(self, start_epoch, end_epoch):
        """
        Return elements of the buffer that fall within the given time-range
        :param start_epoch:
        :param end_epoch:
        :return: list
        """
        start_index = self.closest_timestamp_index(start_epoch)
        end_index = self.closest_timestamp_index(end_epoch)
        return self._buffer[start_index:end_index]


class BufferServiceNode:
    def __init__(self, topic, buffer_size, buffer_size_type):
        self.buffer = Buffer(buffer_size, buffer_size_type)
        self.topic = topic
        master_topics_graph = rosgraph.Master('/rostopic')
        topics = master_topics_graph.getTopicTypes()

        self.msg_type = None
        for (t, t_type) in topics:
            if t == topic:
                self.msg_type = roslib_message.get_message_class(t_type)
                break
        if self.msg_type is None:
            print "Topic: %s wasn't found in the current topics." % topic
            return
        self.subscriber = rospy.Subscriber(topic, self.msg_type, self.subscriber_callback)
        self.service = rospy.Service('buffer_service' + topic, BufferSrv, self.request_handler)
        rospy.spin()

    def subscriber_callback(self, data):
        self.buffer.add(data)

    @staticmethod
    def to_json(data_list):
        return json.dumps([message_conversion.extract_values(data.msg) for data in data_list])

    def request_handler(self, data):
        start_time_epoch = data.start_time.to_sec()
        end_time_epoch = data.end_time.to_sec()
        # Check that we have the data for the requested timeframe
        if self.buffer.check_range(start_time_epoch, end_time_epoch):
            messages = self.buffer.buffer_range(start_time_epoch, end_time_epoch)
            return BufferSrvResponse(0, self.to_json(messages))
        else:
            return BufferSrvResponse(1, "Requested time-range has not been buffered")


if __name__ == "__main__":
    rospy.init_node('buffer_service')
    try:
        topic = rospy.get_param('~topic')
        buffer_size = rospy.get_param('~buffer_size')
        buffer_size_type = rospy.get_param('~buffer_size_type', None)
        if buffer_size_type is None or buffer_size_type not in ['time', 'count']:
            buffer_size_type = 'time'
        if topic[0] != '/':
            topic = '/' + topic
        b = BufferServiceNode(topic, buffer_size, buffer_size_type)
    except KeyError as e:
        print "Missing argument. Usage: _topic:=<str> _buffer_size:=<int>"
        print e
