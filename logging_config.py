import logging
from logging.handlers import RotatingFileHandler

def get_logger(name):
   logger = logging.getLogger(name)

   if not logger.handlers:
      logger.setLevel(level=logging.DEBUG)

      ch = logging.StreamHandler()
      fh = RotatingFileHandler(filename='logs/job.log',
                               maxBytes=10000,
                               backupCount=3,
                               mode='a')

      formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

      ch.setFormatter(formatter)
      fh.setFormatter(formatter)
      
      logger.addHandler(fh)
      logger.addHandler(ch)

   return logger