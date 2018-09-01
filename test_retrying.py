"""
Created on Sat Aug 25 15:27:41 2018

@author: Robert Hennessy (robertghennessy@gmail.com)
"""

import random
import tenacity as ten
#from tenacity import retry
import datetime as dt
import logging


#logging.basicConfig(filename='test.log', level=logging.INFO, 
#                        format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.FileHandler('hello.log')
logger.addHandler(handler)

# reraise the exception, stop after 
@ten.retry(wait=ten.wait_random_exponential(multiplier=1, max=10),
       reraise=True, stop=ten.stop_after_attempt(5),
       before_sleep=ten.before_sleep_log(logger, logging.DEBUG))
def wait_exponential_jitter():
    print(dt.datetime.now().time().isoformat())
    raise Exception("Fail")
    
wait_exponential_jitter()