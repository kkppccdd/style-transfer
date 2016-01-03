#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on 2015年11月15日

@author: kkppccdd
'''
import os
import logging
import Queue
from worker import Worker,TaskFetcher,OutputPusher


import boto3
import worker

class Master(object):
    '''
        control all workers
    '''
    def __init__(self,params):
        self._params = params
        self._local_task_request_queue = Queue.Queue(256)
        self._local_task_completion_queue = Queue.Queue(1024)
        
        sqs = boto3.resource('sqs')
        self._aws_task_request_queue = sqs.get_queue_by_name(QueueName=self._params['aws_task_request_queue_name'])
        self._aws_task_completion_queue = sqs.get_queue_by_name(QueueName=self._params['aws_task_completion_queue_name'])
        
        self._workers = [];
        self._task_fetcher = None
        self._output_pusher = None
        
        self._prepare_worker(self._params['num_of_worker'])
        self._prepare_task_fetcher()
        self._prepare_output_pusher()
    
    def _prepare_worker(self,num_of_workers=1):
        for num in range(0,num_of_workers):
            worker = Worker(str(num),self._local_task_request_queue,self._local_task_completion_queue)
            self._workers.append(worker)
    def _prepare_task_fetcher(self):
        self._task_fetcher = TaskFetcher(self._aws_task_request_queue,self._local_task_request_queue)
    def _prepare_output_pusher(self,):
        self._output_pusher = OutputPusher(self._local_task_completion_queue,self._aws_task_completion_queue)
    def start(self):
        self._task_fetcher.start()
        #self._task_fetcher.join()
        self._output_pusher.start()
        #self._output_pusher.join()
        for worker in self._workers:
            worker.start()
            #worker.join()
            
    def stop(self):
        self._task_fetcher.stop()
        self._output_pusher.stop()
        for worker in self._workers:
            worker.stop()
if __name__ == "__main__":
    # logging
    LOG_FORMAT = "%(filename)s:%(funcName)s:%(asctime)s.%(msecs)03d -- %(message)s"
    logging.basicConfig(format=LOG_FORMAT, datefmt="%H:%M:%S",level=logging.INFO)



    
    aws_task_request_queue_name = os.environ['AWS_TASK_REQUEST_QUEUE_NAME']
    aws_task_completion_queue_name = os.environ['AWS_TASK_COMPLETION_QUEUE_NAME']
    params ={
             'aws_task_request_queue_name':aws_task_request_queue_name,
             'aws_task_completion_queue_name':aws_task_completion_queue_name,
             'num_of_worker':1
    }
    m = Master(params)
    m.start()
