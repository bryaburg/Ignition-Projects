# 3/18/2020 (David Burns)
# Utilizing for decoupling efforts


import re
from com.inductiveautomation.ignition.common import BasicDataset


# DB Utilities
def run_named_query(query, params = {}):
	project = system.tag.readBlocking('[default]Site/ProjectName')[0].value
	return system.db.runNamedQuery(project, query, params)

	
def run_named_query_proj(query, params = {}):
	return system.db.runNamedQuery(query, params)

# Keep these at the module level, compiling regular expressions can be costly if you have to run it many times
first_re = re.compile('(.)([A-Z][a-z]+)')
all_re = re.compile('([a-z0-9])([A-Z])')


def to_snake_case(str):
	s1 = first_re.sub(r'\1_\2', str)
	return all_re.sub(r'\1_\2', s1).lower()

	
def ds2ol(dataset):
	ls = []
	pyDataSet = system.dataset.toPyDataSet(dataset)
	columns = sorted(pyDataSet.getColumnNames())
	
	# Convert the dataset to an object list
	for row in pyDataSet:	
		obj = {}
		for column in columns:
			value = row[column]
			if isinstance(value, unicode):
				value = value.strip()
				
			obj[to_snake_case(column)] = value
		
		ls.append(obj)
	
	return ls

	
def ol2ds(obj_list, headers = []):
	if not isinstance(obj_list, list):
		raise Exception("Argument is not an ObjectList")
	
	# If you passed in a list, but did not pass in an column headers, manually obtain the headers for the dataset
	if isinstance(obj_list, list) and len(obj_list) > 0 and len(headers) == 0:
		headers = obj_list[0].keys()
		
	rows = []
	for obj in obj_list:
		row = []
		for header in headers:
			row.append(obj[header])
			
		rows.append(row)
	
	return system.dataset.toDataSet(headers, rows)

	
def get_object_key(obj, key):
	try:
		return obj[key]
	except KeyError:
		return None

	
def get_method_names(obj):
	from types import FunctionType
	return [x for (x, y) in obj.__dict__.items() if type(y) == FunctionType and not x.startswith('_')]


def is_in_ol(obj_list, obj_prop, value):
		return any(x[obj_prop] == value for x in obj_list)


def hacksaw(string, splitters, number_of_cuts=-1):
	'''Splits a string using the list of splitters numberOfCuts times.  
		
		Args:
			string (str):
			splitters ([str]):
			number_of_cuts (int or [int]):
		
		Returns:
			[str] : List of split strings.
	'''
	sliced_string = [string]
	
	if type(number_of_cuts) == type([]):
		for splitter, number in zip(splitters, number_of_cuts):
			current_cut = []
			for string in sliced_string:
				current_cut += string.split(splitter, number)
			sliced_string = current_cut
	
	else:
		for splitter in splitters:
			current_cut = []
			for string in sliced_string:
				current_cut += string.split(splitter, number_of_cuts)
			sliced_string = current_cut
	
	return sliced_string