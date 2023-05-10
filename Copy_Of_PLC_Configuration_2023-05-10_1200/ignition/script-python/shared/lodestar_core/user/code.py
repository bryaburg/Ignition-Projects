def get_workcenter_selection(username, line=None):
	results = shared.lodestar_core.utilities.run_named_query('user/get_user_workcenters', {
		'USER_NAME': username,
		'LINE': line
	})
	
	return shared.lodestar_core.utilities.ds2ol(results)