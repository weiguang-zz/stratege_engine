import se
import logging
try:
    raise RuntimeError("error")
except RuntimeError as e:
    logging.error('haha', e)