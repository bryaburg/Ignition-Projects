import unittest
import time
import uuid

'''
	Unit Test Assert Methods:
		assertEqual(first, second, msg=None)
		assertNotEqual(first, second, msg=None)
		assertTrue(expr, msg=None)
		assertFalse(expr, msg=None)
		
		assertIs(first, second, msg=None)
		assertIsNot(first, second, msg=None)
		assertIsNone(expr, msg=None)
		assertIsNotNone(expr, msg=None)
		
		assertIn(first, second, msg=None)
		assertNotIn(first, second, msg=None)
			
		assertIsInstance(obj, cls, msg=None)
		assertNotIsInstance(obj, cls, msg=None)
		
		assertRaises(exception, callable, *args, **kwds)
		assertRaises(exception, *, msg=None)
		assertRaisesRegex(exception, regex, callable, *args, **kwds)
		assertRaisesRegex(exception, regex, *, msg=None)
		
		assertAlmostEqual(first, second, places=7, msg=None, delta=None)
		assertNotAlmostEqual(first, second, places=7, msg=None, delta=None)
		
		assertGreater(first, second, msg=None)
		assertGreaterEqual(first, second, msg=None)
		assertLess(first, second, msg=None)
		assertLessEqual(first, second, msg=None)
		
		assertRegexpMatches(text, regexp, msg=None)
		assertNotRegexpMatches(text, regexp, msg=None)
		
		assertItemsEqual(actual, expected, msg=None)		
		assertMultiLineEqual(first, second, msg=None)
		assertSequenceEqual(seq1, seq2, msg=None, seq_type=None)
		assertListEqual(list1, list2, msg=None)
		assertTupleEqual(tuple1, tuple2, msg=None)
		assertSetEqual(set1, set2, msg=None)
		assertDictEqual(expected, actual, msg=None)
'''
	
def run_single(test_case):
	suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
	results = unittest.TestResult()
	
	execution_start = time.time()
	suite.run(results)
	execution_end = time.time()
	
	test_case_module = test_case.__module__
	test_case_name = test_case.__name__
	test_case_tests = common.utils.get_method_names(test_case)
	test_results_dict = results.__dict__
	test_results = []
	tests_passed = 0
	
	# For each method in the test case, determine if it passed or failed
	for method_name in test_case_tests:
		test_passed = True
		test_failure_reason = ''
		
		# Check for method name in the failures to determine if failed
		for (failed_test, failed_reason) in test_results_dict['failures']:
			if failed_test._testMethodName == method_name:
				test_passed = False
				test_failure_reason = failed_reason
		
		if test_passed:
			tests_passed = tests_passed + 1
		
		test_results.append({'test_full': '%s.%s.%s' % (test_case_module, test_case_name, method_name), 'test_short': method_name, 'passed': test_passed, 'failure_traceback': test_failure_reason})
	
	return {
		'test_case': '%s.%s' % (test_case_module, test_case_name),
		'execution_time': (execution_end - execution_start),
		'total_tests': len(test_results),
		'tests_passed': tests_passed,
		'results': test_results
	}
	
def run_batch(test_cases):
	batch = {
		'uuid': str(uuid.uuid4()), 
		'timestamp': system.date.format(system.date.now(), 'yyyy-MM-dd HH:mm:ss'),
		'execution_time': 0.0,
		'total_tests': 0, 
		'total_passed': 0, 
		'total_failed': 0, 
		'test_cases': []
	}	
	
	for test_case in test_cases:
		result = run_single(test_case)
		
		batch['total_tests'] += result['total_tests']
		batch['total_passed'] += result['tests_passed']
		batch['total_failed'] += result['total_tests'] - result['tests_passed']
		batch['execution_time'] += result['execution_time']
		batch['test_cases'].append(result)
		
	return batch
	
def to_ds(test_cases, timestamp = system.date.format(system.date.now(), 'yyyy-MM-dd HH:mm:ss')):
	headers = ['uuid', 'timestamp', 'test_case', 'execution_time', 'tests_ran', 'tests_passed', 'tests_failed', 'test', 'result', 'failure_reason']
	data = []
		
	tests = test_cases
	if not isinstance(tests, list):
		tests = test_cases['test_cases']
		test_uuid = test_cases['uuid']
	else:
		test_uuid = str(uuid.uuid4())
		
	for test_case in tests:
		test_case_name = test_case['test_case']
		execution_time = str(test_case['execution_time'])
		tests_ran = str(test_case['total_tests'])
		tests_passed = str(test_case['tests_passed'])
		tests_failed = str(test_case['total_tests'] - test_case['tests_passed'])
		
		for result in test_case['results']:
			data.append([test_uuid, timestamp, test_case_name, execution_time, tests_ran, tests_passed, tests_failed, result['test_short'], str(result['passed']), result['failure_traceback']])
		
	return system.dataset.toDataSet(headers, data)
	
def save(results_ds, tag_path = '[default]unit_test_results'):
	# Get the already store results and append the new results
	tag_value = common.tags.get_tags(tag_path).value
	tag_value = system.dataset.appendDataset(tag_value, results_ds)
	
	# Write the new dataset back to the tag
	system.tag.writeBlocking(['[default]unit_test_results'], [tag_value])
		

def print_single(test_case_results):
	print '----------------------------------------------'
	print 'Test Case: %s' % test_case_results['test_case']
	print 'Tests Ran: %s' % str(test_case_results['total_tests'])
	print 'Tests Passed: %s' % str(test_case_results['tests_passed'])
	print 'Tests Failed: %s' % str(test_case_results['total_tests'] - test_case_results['tests_passed'])
	print 'Execution Time: %s sec' % str(test_case_results['execution_time'])
	print ''
	
	for result in test_case_results['results']:
		result_str = 'Passed' if result['passed'] else 'FAILED'
		print '%s ... %s' % (result['test_full'], result_str)
		if not result['passed']:
			print "\n%s" % result['failure_traceback']
			
	print '----------------------------------------------'
	
def print_batch(results_batch):
	for test_case in results_batch['test_cases']:
		print_single(test_case)
		print ''
		
def get_runs(test_runs_ds):		
	tests_ol = common.utils.ds2ol(test_runs_ds)
	
	# Get metrics based on that runs test cases
	items = list(
		map(lambda x: {
			'uuid': x['uuid'], 
			'timestamp': x['timestamp'], 
			'tests_total': x['tests_ran'],
			'tests_passed': x['tests_passed'],
			'tests_failed': x['tests_failed'],
			'execution_time': x['execution_time'],
			'test_case': x['test_case']
		}, tests_ol)
	)
	
	# Make the items a unique set containing only the unique test case items
	items = [dict(p) for p in set(tuple(i.items()) 
				for i in items)]
	
	# Iterate over each unique uuid for the run and add up the total metrics, while also breaking each test_case down to its individual tests and their results
	runs = []
	uuids = list(set(map(lambda x: x['uuid'], items)))
	for uuid in uuids:
		test_totals = 0
		tests_passed = 0
		tests_failed = 0
		test_execution_time = 0.0
		timestamp = ''
		test_cases = []
		
		# Total the metrics
		for item in items:				
			if item['uuid'] == uuid:
				timestamp = item['timestamp']
				test_totals = test_totals + item['tests_total']
				tests_passed = tests_passed + item['tests_passed']
				tests_failed = tests_failed + item['tests_failed']
				test_execution_time += item['execution_time']
				
				# Get the individual test_case tests. This is really slow and horrible way of doing things, IMPROVE
				test_case_tests = get_test_case_tests(uuid, item['test_case'], tests_ol)
				test_cases.append({'test_case': item['test_case'], 'execution_time': item['execution_time'], 'tests_total': item['tests_total'], 'tests_passed': item['tests_passed'], 'tests_failed': item['tests_failed'], 'tests': test_case_tests})
				
		runs.append({'uuid': uuid, 'timestamp': timestamp, 'test_total': test_totals, 'tests_passed': tests_passed, 'tests_failed': tests_failed, 'execution_time': test_execution_time, 'tests_cases': test_cases})
		
	return runs
	
def get_test_case_tests(uuid, test_case, tests_ol):
	tests = []
	for test in tests_ol:
		if test['uuid'] == uuid and test['test_case'] == test_case:
			tests.append({'test': test['test'], 'passed': test['result'], 'failure_reason': test['failure_reason']})
		
	return tests