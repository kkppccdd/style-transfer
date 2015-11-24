# system imports
import logging
import os
import sys
import timeit

import json

# library imports
import threading
import numpy as np
from skimage import img_as_ubyte

import caffe

from style import StyleTransfer

class ArtTask(object):
    def __init__(self,identifier,params):
        self.identifier = identifier
        self.params = params
        self.style_image = None
        self.content_image = None
        self.output_image = None

        
class TaskFetcher(threading.Thread):
    def __init__(self,upstream_task_queue,downstream_task_queue):
        super(TaskFetcher, self).__init__()
        self._upstream_task_queue = upstream_task_queue
        self._downstream_task_queue = downstream_task_queue
        self._stop_flag = False
    def run(self):
        """
            continuously fetch event from upstream queue, wrapper it as ArtTask, the push downstream task queue
        """
        logging.info('start fetch task')
        while(self._stop_flag != True):
            for message in self._upstream_task_queue.receive_messages():
                art_task_request = json.loads(message.body)
                logging.debug('fetched task id: '+art_task_request['identifier'])
                art_task = self._transform_to_art_task(art_task_request)
                
                self._downstream_task_queue.put(art_task)
                
                message.delete()
        
        logging.info('stop fetch task')
    def _transform_to_art_task(self,art_task_request):
        if('params' in art_task_request):
            params = art_task_request['params']
        else:
            params = {
                  "ratio":1e5,
                  "num_iters":1,
                  "length":512,
                  "verbose":False
                  }
        identifier = art_task_request['identifier']
        art_task = ArtTask(identifier,params)
        
        # load style image
        
        # load content image
        
        
        return art_task
    def stop(self):
        """
        stop this fetcher
        """
        self._stop_flag = True
    
class OutputPusher(threading.Thread):
    def __init__(self,upstream_task_queue,downstream_task_queue):
        self._upstream_task_queue = upstream_task_queue
        self._downstream_task_queue = downstream_task_queue
    def run(self):
        """
        listening on upstream task queue, upload output image to cloud storage and push completion event to downstream task queue if get taks event from upstream queue.
        """
        pass
    def stop(self):
        """
        stop this worker
        """
        pass
    

class Worker(threading.Thread):
    def __init__(self,name,upstream_task_queue,downstream_task_queue):
        self._name = name
        self._upstream_task_queue  = upstream_task_queue
        self._downstream_task_queue = downstream_task_queue
        
        self._stop_flag = False
        
        caffe.set_mode_cpu()
        logging.info("Running net on CPU.")
        # initialize style transfer
        self._style_transfer = StyleTransfer('googlenet')
        
        
    def _create_art(self,style_image,content_image,params):
        # perform style transfer
        start = timeit.default_timer()
        n_iters = self._style_transfer.transfer_style(style_image, content_image, length=params["length"], init="mixed", ratio=np.float(params["ratio"]), n_iter=params["num_iters"], verbose=params["verbose"])
        end = timeit.default_timer()
        logging.info("Ran {0} iterations in {1:.0f}s.".format(n_iters, end-start))
        img_out = self._style_transfer.get_generated()
        img_out = img_as_ubyte(img_out)
        return img_out;
    def stop(self):
        self._stop_flag = True
    def run(self):
        '''
        '''
        logging.info("Runing worker ("+self._name+")...")
        while self._stop_flag == False:
            task = self._upstream_task_queue.get(False,)
            # validate task
            if self._validate_task(task) == False:
                self._upstream_task_queue.task_done()
                continue
            # create art
            task.output_image = self._create_art(task.style_image, task.content_image, task.params)
            #send event to downstream
            
            self._downstream_task_queue.put(task)
            self._upstream_task_queue.task_done()
        logging.info("Completed worker ("+self._name+").")
    def _validate_task(self,task):
        return True  