def get_production_parameter(line, shift, day, name):
	result = shared.lodestar_core.utilities.run_named_query_proj('assembly/get_production_parameter', {
		'LINE_CODE': line,
		'SHIFT': shift,
		'DAY': day,
		'NAME': name
	})
	
	return shared.lodestar_core.utilities.ds2ol(result)[0]['value']
	
def set_production_parameter(line, shift, day, name, value, description=None, user=None):
	shared.lodestar_core.utilities.run_named_query('assembly/set_production_parameter', {
		'LINE_CODE': line,
		'SHIFT': shift,
		'DAY': day,
		'NAME': name,
		'VALUE': value,
		'DESCRIPTION': description,
		'USER': user
	})