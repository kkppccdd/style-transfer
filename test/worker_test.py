import unittest

import logging
import time
import json
import Queue
import random
#from skimage import data, io, filters
#from scipy.misc import imsave

import caffe

from worker import Worker

class WorkerTest(unittest.TestCase):


    def setUp(self):
        self._upstream_task_queue = Queue.Queue()
        self._downstream_task_queue = Queue.Queue()
        self._worker = Worker("test-worker",self._upstream_task_queue,self._downstream_task_queue)


    def tearDown(self):
        pass


    def test_create_art(self):
        style_image_path = 'resource/style-image/starry_night.jpg'
        content_image_path = 'test-resource/content-image/face-1.jpg'
        
        # load images
        style_image = caffe.io.load_image(style_image_path)
        content_image = caffe.io.load_image(content_image_path)

        params = {
                  "ratio":1e5,
                  "num_iters":1,
                  "length":512,
                  "verbose":False
                  }

        output_image = self._worker._create_art(style_image, content_image, params)
        
        #io.imshow(output_image)
        #io.show()
        #imsave("/tmp/out.png",output_image)
    def test_run(self):
        pass
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()