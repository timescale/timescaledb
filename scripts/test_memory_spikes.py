# Python script to check if there are memory spikes when running out of 
# order random inserts to TimescaleDB database
import psutil
import time
import sys

MEMORY_CAP = 300 * 1024 * 1024 # absolute memory cap
THRESHOLD_RATIO = 1.5 # ratio above which considered memory spike

# finds processes with name as argument
def find_procs_by_name(name):
    "Return a list of processes matching 'name'."
    ls = []
    for p in psutil.process_iter(attrs=['name']):
    	# print p
        if p.info['name'] == name:
            ls.append(p)
    return ls

# return human readable form of number of bytes n
def bytes2human(n):
    # http://code.activestate.com/recipes/578019
    # >>> bytes2human(10000)
    # '9.8K'
    # >>> bytes2human(100001221)
    # '95.4M'
    symbols = ('K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    prefix = {}
    for i, s in enumerate(symbols):
        prefix[s] = 1 << (i + 1) * 10
    for s in reversed(symbols):
        if n >= prefix[s]:
            value = float(n) / prefix[s]
            return '%.1f%s' % (value, s)
    return "%sB" % n

# prints pid of processes
def print_pid(process):
	if not process:
		return
	for p in process:
		print p.pid,
	print
	return

# return process id of new postgres process created when running SQL file
def find_new_process():
	# get postgres processes that are running before insertion starts
	base_process = find_procs_by_name('postgres')
	print 'Processes running before inserts run: '
	print_pid(base_process)

	process_count = len(base_process)

	time.sleep(10) # wait 10 seconds to get process that runs the inserts

	# continuously check for creation of new postgres process
	timeout = time.time() + 60
	while True:
		# prevent infinite loop
		if time.time() > timeout:
			print 'Timed out on finding new process, should force quit SQL inserts'
			sys.exit(4)
		process = find_procs_by_name('postgres')
		cnt = len(process)
		if cnt > process_count:
			process = find_procs_by_name('postgres')
			new_process = (set(process) - set(base_process)).pop()
			return new_process.pid
		time.sleep(1)

def main():
	# get process created from running insert test sql file
	pid = find_new_process()
	p = psutil.Process(pid)
	print 'New process running random inserts: ', pid

	print 'Waiting 1 minute for memory consumption to stabilize'
	time.sleep(60) # wait 1 minute for memory consumption to stabilize

	sum = 0
	for i in range (0, 5):
		sum += p.memory_info().rss
		time.sleep(3)
	avg = sum / 5
	print 'Average memory consumption: ', bytes2human(avg)

	upper_threshold = min(MEMORY_CAP, avg * THRESHOLD_RATIO)

	# check if memory consumption goes above threshold until process terminates every 5 seconds
	timeout = time.time() + 30 * 60
	while True:
		# insert finished
		if not psutil.pid_exists(pid):
			break
		# prevent infinite loop
		if time.time() > timeout:
			print 'Timed out on running inserts'
			p.kill()
			sys.exit(4)

		rss = p.memory_info().rss
		print 'Memory used by process ' + str(p.pid) + ': ' + bytes2human(rss)

		# exit with error if memory above threshold
		if rss > upper_threshold:
			print 'Memory consumption exceeded upper threshold'
			p.kill()
			sys.exit(4)

		time.sleep(30)

	print 'No memory errors detected with out of order random inserts'
	sys.exit(0) # success

if __name__ == '__main__':
	main()