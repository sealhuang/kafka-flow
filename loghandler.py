# vi: set ft=python sts=4 ts=4 sw=4 et:

import os
import gzip
import shutil
from logging.handlers import TimedRotatingFileHandler


class TimedRotatingCompressedFileHandler(TimedRotatingFileHandler):
    """Extended version of TimedRotatingFileHandler that compress 
    logs on rollover.
    """
    def __init__(self, filename='', when='W6', interval=1,
                 backup_count=50, encoding='utf-8'):
        super(TimedRotatingCompressedFileHandler, self).__init__(
            filename=filename,
            when=when,
            interval=int(interval),
            backupCount=int(backup_count),
            encoding=encoding,
        )

    def doRollover(self):
        super(TimedRotatingCompressedFileHandler, self).doRollover()
        log_dir = os.path.dirname(self.baseFilename)
        to_compress = [
            os.path.join(log_dir, f) for f in os.listdir(log_dir)
            if f.startswith(
                os.path.basename(os.path.splitext(self.baseFilename)[0])
            ) and not f.endswith(('.gz', '.json'))
        ]
        for f in to_compress:
            if os.path.exists(f):
                with open(f, 'rb') as _old, gzip.open(f+'.gz', 'wb') as _new:
                    shutil.copyfileobj(_old, _new)
                os.remove(f)


