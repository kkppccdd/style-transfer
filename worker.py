# system imports
import logging
import os
import sys
import timeit

# library imports
import numpy as np
from skimage import img_as_ubyte

import caffe

from style import StyleTransfer

class ArtTask(object):
    def __init__(self,params):
        self.params = params
        self.style_image = None
        self.content_image = None
        self.output_image = None
    

class Worker(object):
    def __init__(self,upstream_task_queue,downstream_task_queue):
        self._upstream_task_queue  = upstream_task_queue
        self._downstream_task_queue = downstream_task_queue
        
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
        #img_out = img_as_ubyte(img_out)
        return img_out;
    
    def start(self):
        '''
        start worker thread
        '''
        pass
    
    def _run(self):
        '''
        '''
        while True:
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
            
    def _validate_task(self,task):
        return True  