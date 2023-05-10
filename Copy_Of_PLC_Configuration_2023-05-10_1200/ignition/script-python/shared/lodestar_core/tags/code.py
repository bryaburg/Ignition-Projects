'''Common Tag Related Functions

	Created by: ???
	Updated by: WJF - 2021-05-03 - Added make_qualified_value
'''


from com.inductiveautomation.ignition.common.sqltags.model.types import DataQuality
from com.inductiveautomation.ignition.common.sqltags import BasicTagValue
from com.inductiveautomation.ignition.common import BasicDataset
from shared.lodestar_core.utilities import *


def get_tags(paths):
	if isinstance(paths, list):
		return system.tag.readAll(paths)
	elif isinstance(paths, str):
		return system.tag.read(paths)
	else:
		return []

def get_udt(path):
	obj = {}

	udt_paths = system.tag.browseTagsSimple(path, 'ASC')	
	for path in udt_paths:
		tag_paths = []
		tag_paths.append(str(path.fullPath))

		tag = system.tag.readAll(tag_paths)
		tag_value = tag[0].value
		tag_name = path.name
		
		if path.isUDT():
			obj[tag_name] = get_udt(path['fullPath'])
		else:
			obj[tag_name] = tag_value
			
	return obj
	
def update_udt(path, obj):
	if not isinstance(obj, dict):
		raise Exception('Passed in value is not a UDT object')
	else:		
		tags = []
		values = []
		
		# Loop through the udt path structure
		udt_paths = system.tag.browse(path, filter={})
		for path in udt_paths.getResults():
			# Append to the list of tags to update
			tags.append(str(path['fullPath']))
			value = shared.lodestar_core.utilities.get_object_key(obj, path['name'])
			if value == None:
				raise Exception("Object does not match UDT schema")
			
			# If the current path has children (another udt/folder) recursively update that udt with the object
			# at the objects key, if the value of the key is not a dictionary then it does match the udt schema
			# since we updated that udt continue with the remaining tag paths
			if path['hasChildren'] and isinstance(value, dict):
				update_udt(path('fullPath', value))
				continue
			elif path['hasChildren'] and not isinstance(value, dict):
				raise Exception("Object does not match UDT schema")				
			elif isinstance(value, str):
				value = str(value)
			
			# Append to the list of tag values to write to the corresponding tag path
			values.append(value)

		system.tag.writeBlocking(tags, values)
				
def tags2obj(tags, values):
	obj = {}
	for i in range(len(tags)):
		tag = shared.lodestar_core.utilities.to_snake_case(tags[i])
		if isinstance(values[i].value, BasicDataset):
			obj[tag] = ds2ol(values[i].value)
		else:
			obj[tag] = values[i].value
			
	return obj


def make_qualified_value(value, quality=DataQuality.GOOD_DATA, timestamp=None):
	'''Creates a qualified value from given parameters.
		
		Args:
			value (anything): Can be (just about) anything. Limit not yet found.
			timestamp (java.util.Date): Timestamp when the value was set.
			quality (com.inductiveautomation...DataQuality): Good, Bad, or Stale data.
		
		Returns:
			qv (com.inductiveautomation...BasicTagValue): Contains value, timestamp, quality.
		
		TODO:  WJF - Find a good reference for a longer 'qualified value' explaination. 
	'''
	qv = BasicTagValue()
	
	qv.setValue(value)
	qv.setQuality(quality)
	if timestamp:
		qv.setTimestamp(timestamp)
	
	return qv