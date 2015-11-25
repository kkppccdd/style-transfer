#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on 2015年11月26日

@author: kkppccdd
'''
import unittest

import os
import logging
import json
import Queue
import random
import time
import datetime

import boto3
import numpy
import caffe

from worker import OutputPusher, ArtTask

# logging
LOG_FORMAT = "%(filename)s:%(funcName)s:%(asctime)s.%(msecs)03d -- %(message)s"
logging.basicConfig(format=LOG_FORMAT, datefmt="%H:%M:%S", level=logging.INFO)


class OutputPusherTest(unittest.TestCase):


    def setUp(self):
        sqs = boto3.resource('sqs')
        aws_task_completion_queue_name = 'task-completion-test-' + str(random.randint(1, 100))
        self._aws_task_completion_queue = sqs.create_queue(QueueName=aws_task_completion_queue_name)
        logging.info('create sqs queue: ' + self._aws_task_completion_queue.url)
        self._local_task_output_queue = Queue.Queue(10)

        self._output_pusher = OutputPusher(self._local_task_output_queue,self._aws_task_completion_queue)
        self._output_pusher.start()
    def tearDown(self):
        self._output_pusher.stop()
        self._output_pusher.join()
        if self._aws_task_completion_queue is not None:
            self._aws_task_completion_queue.delete()


    def test_should_send_completion_given_aws_queue_when_completed(self):
        #construct
        params = {
                  "ratio":1e5,
                  "num_iters":1,
                  "length":512,
                  "verbose":False
                  }
        art_task = ArtTask('id12345',params)
        art_task.content_image = caffe.io.load_image('test-resource/content-image/face-1.jpg')
        art_task.style_image = caffe.io.load_image('resource/style-image/starry_night.jpg')
        art_task.output_image = caffe.io.load_image('test-resource/content-image/face-1.jpg')
        
        self._local_task_output_queue.put_nowait(art_task)
        time.sleep(30)
        
        # check 
        messages = self._aws_task_completion_queue.receive_messages()
        self.assertEqual(1, len(messages))
        completion_message = messages[0]
        completion = json.loads(completion_message.body)
        self.assertEquals('id12345',completion['identifier'])
        self.assertIsNotNone(completion['output_image_url'])
        
    def test_should_upload_output_image_given_aws_queue_when_completed(self):
        #construct
        params = {
                  "ratio":1e5,
                  "num_iters":1,
                  "length":512,
                  "verbose":False
                  }
        art_task = ArtTask('id'+str(random.randint(1000000, 9999999)),params)
        art_task.content_image = caffe.io.load_image('test-resource/content-image/face-1.jpg')
        art_task.style_image = caffe.io.load_image('resource/style-image/starry_night.jpg')
        art_task.output_image = caffe.io.load_image('test-resource/content-image/face-1.jpg')
        
        self._local_task_output_queue.put_nowait(art_task)
        time.sleep(30)
        
        # verify
        s3_bucket = os.environ['S3_IMAGE_BUCKET']
        output_image_url = 'http://'+s3_bucket+'.s3.amazonaws.com/' + datetime.date.today().strftime('%Y%m%d') + '/'+art_task.identifier+'/'+art_task.identifier+'-output.jpg'
        output_image = caffe.io.load_image(output_image_url)
        self.assertIsNotNone(output_image)
        logging.info('verify uploaded image pass')
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()