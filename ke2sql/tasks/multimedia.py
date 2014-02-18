#!/usr/bin/env python
# encoding: utf-8
"""
Created by 'bens3' on 2013-06-21.
Copyright (c) 2013 'bens3'. All rights reserved.
"""

from ke2psql.tasks.ke import KEDataTask
from ke2psql.model.keemu import *


class MultimediaTask(KEDataTask):

    model_class = MultimediaModel
    module = 'emultimedia'

    def process(self, data):

        # Ke Emu has mime_types:  application, message, video, x-url, text, image
        # Application (including ms word) has stuff we definitely don't want released - messages & text possible risk too
        # If it's not an image or video, skip it
        if data.get('MulMimeType', None) not in ('image', 'video'):
            return

        super(MultimediaTask, self).process(data)





