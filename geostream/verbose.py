
import sys


def console_progress(taskname, i, perc, total):
    string = ''
    if taskname:
        string += '{}. '.format(taskname)
    string += 'Progress: {:d}'.format(i)
    if perc and total:
        string += ' of {:d} ({:.1f}%)'.format(total, perc)
    string += '\r'
    
    sys.stdout.write(string)
    sys.stdout.flush()


def idle_progress(taskname, i, perc, total):
    string = ''

    if i == 0 and taskname:
        string += '{}. \n'.format(taskname)
    
    string += 'Progress: {:d}'.format(i)
    if perc and total:
        string += ' of {:d} ({:.1f}%%)'.format(total, perc)
    string += '\n'
    
    sys.stdout.write(string)
    sys.stdout.flush()


def track_progress(iterator, taskname=None, every=10000, total=None, callback=idle_progress):
    if hasattr(iterator, '__len__'):
        total = len(iterator)
    
    if not every and total:
        every = total / 100.0 # every 1 percent

    if total:
        perc = 0
        percincr = (every / float(total)) * 100

    # initial reporting
    if total:
        callback(taskname, 0, 0, total)
    else:
        callback(taskname, 0, 0, 0)

    # iterate
    nxt = incr = every
    for i,item in enumerate(iterator):
        if i >= nxt:
            nxt += every
            if total:
                perc += percincr
                callback(taskname, i+1, perc, total)
            else:
                callback(taskname, i+1, None, None)
        yield item

    # final reporting
    if total:
        callback(taskname, i+1, 100, total)
    else:
        callback(taskname, i+1, 0, 0) 
