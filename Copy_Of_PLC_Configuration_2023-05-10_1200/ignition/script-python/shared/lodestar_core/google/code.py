def send_to_chat(webhook_url, contents):
	return system.net.httpPost(webhook_url, 'application/json; charset=UTF8', contents)
	
def send_unittest_results(webhook_url, results_batch):
	result = "<b><font color=\"#009409\">Passed</font></b>" if results_batch['total_passed'] == results_batch['total_tests'] else "<b><font color=\"#940000\">Failed</font></b>"
	header = {
		'title': 'Lodestar UnitBot',
		'subtitle': 'The Magnificent Lodestar Quality Controller'
	}
	
	sections = [{
		'widgets':[{
			'keyValue': {
				'topLabel': 'Run UUID',
				'content': '<a href="https://awn-mesappd8.whirlpool.com/system/unittests/{}">{}</a>'.format(results_batch['uuid'], results_batch['uuid'])
			}}, {
			'keyValue': {
				'topLabel': 'Timestamp',
				'content': results_batch['timestamp']
			}}, {
			'keyValue': {
				'topLabel': 'Execution Time',
				'content': "{}sec".format(round(results_batch['execution_time'], 2))
			}
		}]
	},{
		'widgets': [{
			'keyValue': {
				'topLabel': 'Result',
				'content': result
			}},{
			'keyValue': {
				'topLabel': 'Total Passed',
				'content': '{} / {}'.format(results_batch['total_passed'], results_batch['total_tests'])
			}
		}]
	}]
	
	failed_tests = []
	for test_case in results_batch['test_cases']:
		for result in test_case['results']:
			if not result['passed']:
				failed_tests.append(result['test_short'])
	
	if len(failed_tests) > 0:
		failed_tests_content = ''
		for test in failed_tests:
			test_html = '<b><font color=\"#940000\">{}</font></b><br/>'.format(test)
			failed_tests_content += test_html
		
		failed_tests_content = failed_tests_content[:-5]
		sections[1]['widgets'].append({'keyValue':{'topLabel': 'Failed Tests', 'content': failed_tests_content}})
	
	content = system.util.jsonEncode({'cards': [{'header': header, 'sections': sections}]})
	return send_to_chat(webhook_url, content)