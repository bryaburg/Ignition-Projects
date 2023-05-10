def benchmark(func, passes=10, print_progress=False):
	times = [system.date.now()]
	faults = []
	for i in range(passes):
		try:
			func()
		except e:
			timestamp = system.date.now().toInstant()
			fautls.append((timestamp, e))
			
			print '%s - Fault' % timestamp
			pass
		finally:
			times.append(system.date.now())
			
		if print_progress:
			if not i % 10:
				print "Pass: %s - %s" % (str(i), str(system.date.now().toInstant()))
	
	deltas = [b.toInstant().toEpochMilli()-a.toInstant().toEpochMilli() for a,b in zip(times[:-1], times[1:])]
	return (deltas, faults)