#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on 2015年11月21日

@author: kkppccdd
'''
import unittest

from master import Master

class Test(unittest.TestCase):
    """
        test master
    """

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def testMasterInit(self):
        params = {
                  "num_of_worker":1,
                  "sqs_request_queue_name":"task-request-dev",
                  "sqs_completion_queue_name":"task-completion-dev"
        }
        
        master = Master(params)
        
        self.assertIsNotNone(master, "create master")
    def testTaskFetch(self):
        params = {
                  "num_of_worker":0,
                  "sqs_request_queue_name":"task-request-dev",
                  "sqs_completion_queue_name":"task-completion-dev"
        }
        
        master = Master(params)
        
        # generate task event to queue
        
        # wait a moement
        
        # check if generate art task event
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()