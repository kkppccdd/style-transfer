#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on 2015年11月15日

@author: kkppccdd
'''

import Queue
from worker import Worker,TaskFetcher,OutputPusher


import boto3

class Master(object):
    '''
        control all workers
    '''
    def __init__(self,params):
        self._params = params
        self._requestQueue = Queue.Queue(256)
        self._outputQueue = Queue.Queue(1024)
        
        self._workers = [];
        self._task_fetcher = None
        self._output_pusher = None
        
        self._prepare_worker(self._params['num_of_worker'])
        self._prepare_task_fetcher()
        self._prepare_output_pusher()
    
    def _prepare_worker(self,num_of_workers=1):
        for num in range(0,num_of_workers):
            worker = Worker(str(num),self._requestQueue,self._outputQueue)
            self._workers.append(worker)
    def _prepare_task_fetcher(self):
        # get upstream queue
        sqs = boto3.resource('sqs')
        aws_task_queue = sqs.get_queue_by_name(QueueName=self._params["sqs_request_queue_name"])
        self._task_fetcher = TaskFetcher(aws_task_queue,self._requestQueue)
    def _prepare_output_pusher(self,):
        # get downstream qeuue
        sqs = boto3.resource('sqs')
        aws_completion_event_queue = sqs.get_queue_by_name(QueueName=self._params["sqs_completion_queue_name"])
        self._output_pusher = OutputPusher(self._outputQueue,aws_completion_event_queue)
    def start(self):
        self._task_fetcher.start()
        self._output_pusher.start()
        
        for worker in self._workers:
            worker.start()
            
    def stop(self):
        pass
    