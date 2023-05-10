def get_sublines(line):
	results = shared.lodestar_core.utilities.run_named_query_proj('production_model/get_lines_by_line', {'LINE': line})
	return shared.lodestar_core.utilities.ds2ol(results)