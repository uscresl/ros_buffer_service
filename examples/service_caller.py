#!/usr/bin/env python
PKG='ros_buffer_service'
import roslib; roslib.load_manifest(PKG)
import rospy

from ros_buffer_service.srv import *
rospy.init_node('buffer_service_caller_example')

rospy.wait_for_service('buffer_service/test')
service = rospy.ServiceProxy('buffer_service/test', BufferSrv)
r = BufferSrvRequest(start_time=rospy.Time.now()-rospy.Duration(5),end_time=rospy.Time.now())
print r
res = service(r)
print res
