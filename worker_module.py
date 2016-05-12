#!/usr/bin/python
import boto3
import thread
import time
import cv2
from boto3.s3.transfer import S3Transfer

# variable responsible for a dalay everywhere
_DELAY = 20
BUCKET = 'web-module-files'
# Open, rotate(90') and save image
def rotate_image(name):
    img = cv2.imread(name, 0)
    rows,cols = img.shape
    M = cv2.getRotationMatrix2D((cols/2,rows/2),90,1)
    dst = cv2.warpAffine(img,M,(cols,rows))
    cv2.imwrite(name, dst)

# Define a function for the thread
def receive_message_sqs( name, queue, region, delay):
    sqs = boto3.resource('sqs', region_name='us-west-2')
    queue = sqs.get_queue_by_name(QueueName=queue)
    s3 = boto3.client('s3')
    transfer = S3Transfer(s3)
    while (1):
        for message in queue.receive_messages():
            print "Message body: %s" %message.body
            transfer.download_file(BUCKET, message.body, message.body)
            rotate_image(message.body)
            transfer.upload_file(message.body, BUCKET, message.body)
            print "Send file after rotation"
        time.sleep(delay)

# Create two threads as follows
try:
    thread.start_new_thread( receive_message_sqs,
            ('consumer-thread', 'web-module','us-west-2', _DELAY))
    print "New receive msg thread start"
except:
    print 'Error: unable to start thread'

while (1):
    pass

