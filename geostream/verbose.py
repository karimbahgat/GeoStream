
import sys
from time import time


def console_progress(taskname, i, perc, total, elapsed=None):
    string = ''
    if taskname:
        string += '{}. '.format(taskname)
    string += 'Progress: {:d}'.format(i)
    if perc and total:
        string += ' of {:d} ({:.1f}%)'.format(total, perc)
    string += '\r'
    
    sys.stdout.write(string)
    sys.stdout.flush()

    if perc >= 100 and elapsed:
        sys.stdout.write('Completed in {:.3f} seconds \n'.format(elapsed))
        sys.stdout.flush()


def idle_progress(taskname, i, perc, total, elapsed=None):
    string = ''

    if i == 0 and taskname:
        string += '{}. \n'.format(taskname)
    
    string += 'Progress: {:d}'.format(i)
    if perc and total:
        string += ' of {:d} ({:.1f}%)'.format(total, perc)
    string += '\n'
    
    sys.stdout.write(string)
    sys.stdout.flush()

    if perc >= 100 and elapsed:
        sys.stdout.write('Completed in {:.3f} seconds \n'.format(elapsed))
        sys.stdout.flush()



def i_to_filepos(i, fileobj, end=None):
    "Convert i to byte position in file"
    # SIMPLY INSERT INTO progress() AS ARBITRARY CALLABLE
    # NOT FINISHED YET...
    if not end and i == 0:
        fileobj.seek(0, 2)
        end = fileobj.tell()
    i = fileobj.tell()
    perc = i/float(end)
    return i, end, perc



def track_progress(iterator, taskname=None, every=None, total=None, callback=idle_progress):
    if hasattr(iterator, '__len__'):
        total = len(iterator)
    
    if not every and total:
        every = total / 10.0 # every 1 percent

    if not every:
        every = 10000

    if total:
        perc = 0
        percincr = (every / float(total)) * 100

    # initial reporting
    if total:
        callback(taskname, 0, 0, total)
    else:
        callback(taskname, 0, 0, 0)

    # iterate
    t = time()
    nxt = incr = every
    for i,item in enumerate(iterator):
        # yield item
        yield item

        # report if passed threshold
        if i >= nxt:
            nxt += every
            if total:
                perc += percincr
                callback(taskname, i, perc, total)
            else:
                callback(taskname, i, None, None)

    # final reporting
    elapsed = time()-t
    if total:
        callback(taskname, i+1, 100, total, elapsed)
    else:
        callback(taskname, i+1, 100, i+1, elapsed)


        
