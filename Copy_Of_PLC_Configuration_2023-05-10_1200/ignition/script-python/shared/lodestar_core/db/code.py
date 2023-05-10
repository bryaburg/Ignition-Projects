def run_named_query(query, params = {}, is_get_query = True):
	project = system.tag.readBlocking('[default]Site/ProjectName')[0].value
	return system.db.runNamedQuery(project, query, params)