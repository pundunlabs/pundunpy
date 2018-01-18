import logging

def setup_logging(level = logging.WARNING):
    date_fmt = '%Y-%m-%d %H:%M:%S'
    log_fmt=('%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] '
             '%(levelname)s %(message)s'
            )
    logging.basicConfig(format=log_fmt, level=level, datefmt=date_fmt)
