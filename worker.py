# system imports
import logging
import os
import sys
import timeit
import time
import datetime

import json

# library imports
import threading
from Queue import Queue,Empty
import numpy as np
from skimage import img_as_ubyte
from scipy.misc import imsave

import caffe
import boto3

from style import StyleTransfer

STYLE_IMAGE_ROOT = 'resource/style-image'
STYLE_IMAGE_MAPPING={
                     'starry_night':'starry_night.jpg'
                     }

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write("%s %s / %s (%.2f%%)\n" % (self._filename, self._seen_so_far,self._size, percentage))
            sys.stdout.flush()
                             
class ArtTask(object):
    def __init__(self,id,params):
        self._id = id
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
                try:
                    art_task_request = json.loads(message.body)
                    logging.debug('fetched task id: '+art_task_request['_id'])
                    art_task = self._transform_to_art_task(art_task_request)
                    logging.info('fecthed task: '+art_task._id)
                    self._downstream_task_queue.put(art_task)
                except Exception,ex:
                    logging.error(ex)
                message.delete()
            logging.info('task fetcher sleep 5 seconds')
            time.sleep(5)
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
        id = art_task_request['_id']
        art_task = ArtTask(id,params)
        
        # load style image
        style_image_path = STYLE_IMAGE_ROOT + '/' + STYLE_IMAGE_MAPPING[art_task_request['style']]
        style_image = caffe.io.load_image(style_image_path)
        art_task.style_image = style_image
        # load content image
        content_image = caffe.io.load_image(art_task_request['contentImageUrl'])
        art_task.content_image = content_image
        
        return art_task
    def stop(self):
        """
        stop this fetcher
        """
        self._stop_flag = True
    
class OutputPusher(threading.Thread):
    def __init__(self,upstream_task_queue,downstream_task_queue):
        super(OutputPusher, self).__init__()
        self._upstream_task_queue = upstream_task_queue
        self._downstream_task_queue = downstream_task_queue
        self._s3_image_bucket_name = os.environ['S3_IMAGE_BUCKET']
        self._stop_flag = False
    def run(self):
        """
        listening on upstream task queue, upload output image to cloud storage and push completion event to downstream task queue if get taks event from upstream queue.
        """
        logging.info('start output pusher.')
        while(self._stop_flag != True):
            try:
                art_task = self._upstream_task_queue.get_nowait()
                output_image_url = self._upload_output_image(art_task)
                self._notify_completion(art_task,output_image_url)
            except Empty:
                logging.info('not completed task, wait for 5 second then retry.')
                time.sleep(5)
            
    def stop(self):
        """
        stop this worker
        """
        self._stop_flag = True
    def _upload_output_image(self,art_task):
        # save output image to tmp
        tmp_file_path = '/tmp/'+ art_task._id+'-output.tmp.jpg'
        imsave(tmp_file_path,art_task.output_image)
        output_image_key = datetime.date.today().strftime('%Y%m%d') + '/'+art_task._id+'/'+art_task._id+'-output.jpg'
        s3_client = boto3.resource('s3')
        s3_client.meta.client.upload_file(tmp_file_path,self._s3_image_bucket_name,output_image_key,ExtraArgs={'ContentType': 'image/jpeg','ACL': 'public-read'},Callback=ProgressPercentage(tmp_file_path))
        output_image_url='http://'+self._s3_image_bucket_name+'.s3.amazonaws.com/' + output_image_key
        
        return output_image_url
    def _notify_completion(self,art_task,output_image_url):
        art_task_completion = {
                               '_id':art_task._id,
                               'outputImageUrl':output_image_url
                               }
        self._downstream_task_queue.send_message(MessageBody=json.dumps(art_task_completion))

class Worker(threading.Thread):
    def __init__(self,name,upstream_task_queue,downstream_task_queue):
        super(Worker, self).__init__()
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
            try:
                task = self._upstream_task_queue.get(False)
                # validate task
                if self._validate_task(task) == False:
                    self._upstream_task_queue.task_done()
                    continue
                # create art
                task.output_image = self._create_art(task.style_image, task.content_image, task.params)
                #send event to downstream
                self._downstream_task_queue.put(task)
                self._upstream_task_queue.task_done()
            except Empty:
                logging.info('not task request on local queue, retry in 5 seconds later.')
                time.sleep(5)
            
        logging.info("Completed worker ("+self._name+").")
    def _validate_task(self,task):
        return True  