#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on 2015年11月24日

@author: kkppccdd
'''
import unittest

import logging
import json
import Queue
import random
#from skimage import data, io, filters
#from scipy.misc import imsave

import boto3
import numpy


from worker import TaskFetcher

# logging
LOG_FORMAT = "%(filename)s:%(funcName)s:%(asctime)s.%(msecs)03d -- %(message)s"
logging.basicConfig(format=LOG_FORMAT, datefmt="%H:%M:%S", level=logging.INFO)

class TaskFetcherTest(unittest.TestCase):
    def setUp(self):
        sqs = boto3.resource('sqs')
        aws_task_request_queue_name = 'task-request-test-' + str(random.randint(1, 100))
        self._aws_task_request_queue = sqs.create_queue(QueueName=aws_task_request_queue_name)
        logging.info('create sqs queue: ' + self._aws_task_request_queue.url)
        self._local_task_request_queue = Queue.Queue(10)
        
        self._task_fetcher = TaskFetcher(self._aws_task_request_queue,self._local_task_request_queue)
        
        self._task_fetcher.start()

    def tearDown(self):
        self._task_fetcher.stop()
        self._task_fetcher.join()
        if self._aws_task_request_queue is not None:
            self._aws_task_request_queue.delete()
            
    def test_should_get_task_given_task_request_queue_when_send_task_request(self):
        # aws queue is created

        # given task request to aws task request queue
        art_task_request = {'_id':'id12345',
                            'contentImageUrl':'https://www.baidu.com/img/bd_logo1.png',
                            'style':'starry_night',
                            'params':{
                  "ratio":1e5,
                  "num_iters":10,
                  "length":512,
                  "verbose":False
                  }}

        
        self._aws_task_request_queue.send_message(MessageBody=json.dumps(art_task_request))

        
        
        art_task = self._local_task_request_queue.get(True, 30)
        
        self.assertIsNotNone(art_task)
        self.assertEquals('id12345',art_task._id)
        
       
    def test_should_get_task_with_style_image_given_task_request_queue_when_send_task_request(self):
        
        # given task request to aws task request queue
        art_task_request = {'_id':'id12345',
                            'contentImageUrl':'https://www.baidu.com/img/bd_logo1.png',
                            'style':'starry_night',
                            'params':{
                  "ratio":1e5,
                  "num_iters":10,
                  "length":512,
                  "verbose":False
                  }}

        
        self._aws_task_request_queue.send_message(MessageBody=json.dumps(art_task_request))

        art_task = self._local_task_request_queue.get(True, 30)
        
        self.assertIsNotNone(art_task.style_image)
        self.assertEquals(numpy.ndarray,type(art_task.style_image))
    
    def test_should_get_task_with_content_image_given_task_request_queue_when_send_task_request(self):
        # given task request to aws task request queue
        art_task_request = {'_id':'id12345',
                            'contentImageUrl':'https://www.baidu.com/img/bd_logo1.png',
                            'style':'starry_night',
                            'params':{
                  "ratio":1e5,
                  "num_iters":10,
                  "length":512,
                  "verbose":False
                  }}

        
        self._aws_task_request_queue.send_message(MessageBody=json.dumps(art_task_request))

        art_task = self._local_task_request_queue.get(True, 30)
        
        self.assertIsNotNone(art_task.content_image)
        self.assertEquals(numpy.ndarray,type(art_task.content_image))
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
