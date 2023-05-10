from __future__ import with_statement

import os, shutil, re

shared.tools.pretty.install()

context = shared.tools.meta.getDesignerContext()

deserializer = context.createDeserializer()

global_project = context.getGlobalProject().getProject()
designer_project = context.getProject()

global_resources = dict(
	('%s/%s' % (resource.getResourceType(), global_project.getFolderPath(resource.getResourceId())) or '', resource)
	for resource
	in global_project.getResources()
	)

project_resources = dict(
	('%s/%s' % (resource.getResourceType(), designer_project.getFolderPath(resource.getResourceId())) or '', resource)
	for resource
	in designer_project.getResources()
	)

global_resource_types = set([
	resource.getResourceType() 
	for resource 
	in global_resources.values()
	])

project_resource_types = set([
	resource.getResourceType() 
	for resource 
	in project_resources.values()
	])



#>>> design_types
#"design_types" <'set'> of 8 elements
#   0?  |    u'client_tags'
#   1?  |    u'component-template'
#   2?  |    u'named-query'
#   3?  |    u'com.ia.report'
#   4?  |    u'__folder'
#   5?  |    u'sr.script.project'
#   6?  |    u'window'
#   7?  |    u'group'
#
#>>> global_types
#"global_types" <'set'> of 15 elements
#    0?  |    u'area'
#    1?  |    u'sr.script.shared'
#    2?  |    u'cell_group'
#    3?  |    u'line'
#    4?  |    u'enterprise'
#    5?  |    u'soapconfiguration'
#    6?  |    u'alarm-pipeline'
#    7?  |    u'main'
#    8?  |    u'cell'
#    9?  |    u'component-template'
#   10?  |    u'wsconsumerssettings'
#   11?  |    u'__folder'
#   12?  |    u'site'
#   13?  |    u'restendpoint'
#   14?  |    u'restconfiguration'

#def extract_global_enterprise(resource_objects):
#	return {}
#
#def extract_global_site(resource_objects):
#	return {}
#
#def extract_global_area(resource_objects):
#	return {}
#
#def extract_global_line(resource_objects):
#	return {}
#
#def extract_global_cell_group(resource_objects):
#	return {}
#
#def extract_global_cell(resource_objects):
#	return {}


#def extract_global_soapconfiguration(resource_objects):
#	return {}
#
#def extract_global_wsconsumerssettings(resource_objects):
#	return {}
#
#def extract_global_restendpoint(resource_objects):
#	return {}
#
#def extract_global_restconfiguration(resource_objects):
#	return {}


#def extract_global_main(resource_objects):
#	return {}

		
#def extract_global_template(resource_objects):
#	return {}





POORSQL_BINARY_PATH = 'C:/Workspace/bin/SqlFormatter.exe'

# from https://stackoverflow.com/a/165662/13229100
from subprocess import Popen, PIPE, STDOUT

def format_sql(raw_sql):
	try:
		raise KeyboardInterrupt
		
		poorsql = Popen(
			[POORSQL_BINARY_PATH,
			], stdout=PIPE, stdin=PIPE, stderr=STDOUT)
			
		formatted = poorsql.communicate(input=raw_sql)[0]

		return formatted.replace('\r\n', '\n').strip()
	except:
		return raw_sql






def getSerializationCauses(exception):
	causes = []
	while exception:
		causes.append(exception)
		exception = exception.getCause()
	return causes
	


import java.awt.Point, java.awt.Dimension, java.util.UUID

BASE_TYPES = set([bool, float, int, long, None, str, unicode])

COERSION_MAP = {
	java.awt.Point: lambda v: {'x': v.getX(), 'y': v.getY()},
	java.awt.Dimension: lambda v: {'width': v.getWidth(), 'height': v.getHeight()},
	java.util.UUID: lambda v: str(v),
	}


def coerceValue(value, default=str):
	if type(value) in BASE_TYPES:
		return value
	else:
		return COERSION_MAP.get(type(value), default)(value)


#ptd = propsetToDict = lambda ps: dict([(p.getName(), ps.get(p)) for p in ps.getProperties()])

def serializeToXML(obj, context=context):
	serializer = context.createSerializer()
	serializer.addObject(obj)
	return serializer.serializeXML()
	

def propsetToDict(property_set, recurse=False, coersion=coerceValue, visited=None):
	if visited is None: 
		visited = set()
	elif property_set in visited:
		return None
	
	result_dict = {}
	for prop in property_set.getProperties():
		value = property_set.get(prop)
		
		if recurse and not type(value) in BASE_TYPES:
			try:
				deep = propsetToDict(value, recurse, coersion, visited)
			except:
				try:
					deep = []
					for element in value:
						try:
							deep.append(propsetToDict(element, recurse, coersion, visited))
						except:
							deep.append(coersion(element))
				except:
					deep = None
			
			if deep:
				value = deep
			else:
				value = coersion(value)			
		else:
			value = coersion(value)
		
		result_dict[prop.getName()] = value
	
	return result_dict


def hashmapToDict(hashmap):
	return dict(
		(key, hashmap.get(key))
		for key in hashmap.keySet()
		)


from com.inductiveautomation.ignition.common.xmlserialization import SerializationException


def extract_global_script(resource_objects):
	assert len(resource_objects) == 1, 'Resource is expected to be contained in one root object'

	script = resource_objects[0]
	
	return {
		'.py': script,
		}

	
		
def extract_project_script(resource_objects):
	assert len(resource_objects) == 1, 'Resource is expected to be contained in one root object'
	
	script = resource_objects[0]
	
	return {
		'.py': script,
		}
	
	
def extract_alarmpipeline(resource_objects):
	assert len(resource_objects) == 1, 'Resource is expected to be contained in one root object'
	
	configuration = propsetToDict(resource_objects[0], recurse=True)
		
	return {
		'.json': system.util.jsonEncode(configuration, 2),
		}


def extract_namedquery(resource_objects):
	assert len(resource_objects) == 1, 'Resource is expected to be contained in one root object'
	
	named_query = resource_objects[0]
	
	info = {
		'query': named_query.getQuery(),
		'database': named_query.getDatabase() or '-default-',
		'parameters': dict(
			(param.getIdentifier(), {
				'sql_type'    : str(param.getSqlType()),
				'type'      : str(param.getType()),
				'identifier': str(param.getIdentifier()),
			}) 
			for param 
			in named_query.getParameters()
		),
		'type': named_query.getType(),
	}

	return {
		'.sql': format_sql(info['query']),
		'.config.json' : system.util.jsonEncode(info, 2),
		}


def extract_window(resource_objects, designer_context=context):
	assert len(resource_objects) == 1, 'Resource is expected to be contained in one root object'
	
	window_info = resource_objects[0]
	
	try:
		window_context = deserializer.deserializeBinary(window_info.getSerializedCode())
	except SerializationException, error:	
		return {
			'.error': '\n'.join([str(e) for e in getSerializationCauses(error)])
			}
	
	window = window_context.getRootObjects()[0]
	
	return {
		'.xml': serializeToXML(window)
		}
		

def extract_template(resource_objects, designer_context=context):
	assert len(resource_objects) == 1, 'Resource is expected to be contained in one root object'

	template_info = resource_objects[0]
	
	try:
		template_context = deserializer.deserializeBinary(template_info.getSerializedBytes())
	except SerializationException, error:	
		return {
			'.error': '\n'.join([str(e) for e in getSerializationCauses(error)])
			}
	
	template = template_context.getRootObjects()[0]
	
	return {
		'.xml': serializeToXML(template)
		}	


def extract_design_report(resource_objects):
	assert len(resource_objects) == 1, 'Resource is expected to be contained in one root object'

	report = resource_objects[0]

	info = {
		'title': report.getTitle(),
		'description': report.getDescription(),
		
	}

	page_template = report.getTemplate()
	template = page_template.getTemplate()

	data_config = report.getDataConfig()
	
	info['parameters'] = [{
			'name': param.getName(),
			'default': param.getDefaultValue(),
			'type': str(param.getType()),
		}
		for param
		in data_config.getParameters()
		]
	
#	datasource_types = {
#		'com.ia.reporting.script-data-type': do_things,
#	
#		}
	
	info['datasources'] = 'INCOMPLETE EXTRACTION'
	
	return {
		'.config.json': system.util.jsonEncode(info, 2),
		'.preview.bmp': report.getSnapshot(),
		'.page.xml': template.toXML().toString(),
		}


def extract_design_clientevents(resource_objects):
	assert len(resource_objects) == 1, 'Resource is expected to be contained in one root object'
	
	client_script_config = resource_objects[0]
	
	scripts = {}
	
	script = client_script_config.getStartupScript()
	if script:
		scripts['startup.py'] = script
	
	script = client_script_config.getShutdownScript()
	if script:
		scripts['shutdown.py'] = script
	
	script = client_script_config.getShutdownAllowedScript()
	if script:
		scripts['shutdown-intercept.py'] = script

	key_schema_pattern = re.compile("(\[(?P<modifiers>.*)\] )?(?P<key>.*) \((?P<action>.*)\)")
	key_modifier_pattern = re.compile("(Button \d|\w+)")
	
	key_scripts = client_script_config.getKeyScripts()
	for kix, key_script in enumerate(key_scripts):
		key_config = key_schema_pattern.match(key_script.getDisplay()).groupdict()
		scripts['key/%s.py' % key_script.getDisplay()] = key_scripts[key_script]
		scripts['key/%s.config.json' % key_script.getDisplay()] = system.util.jsonEncode({
			'action': key_config['action'],
			'key': key_config['key'].replace("'", ''),
			'modifiers': key_modifier_pattern.findall(key_config['modifiers']) if key_config['modifiers'] else []
			}, 2)
	
	timer_scripts = client_script_config.getTimerScripts()
	for timer_script in timer_scripts:
		scripts['timer/%s.py' % timer_script.getName()] = timer_scripts[timer_script]
		scripts['timer/%s.config.json' % timer_script.getName()] = system.util.jsonEncode({
			'enabled': timer_script.isEnabled(),
			'timing': 'delay' if timer_script.isFixedDelay() else 'rate',
			'period': timer_script.getDelay(),
			'threading': 'shared' if timer_script.isSharedThread() else 'dedicated',
			}, 2)
	
	for tag_script in client_script_config.getTagChangeScripts():
		scripts['tag-change/%s.py' % tag_script.getName()] = tag_script.getScript()
		scripts['tag-change/%s.config.json' % tag_script.getName()] = system.util.jsonEncode({
			'name': tag_script.getName(),
			'tags': [tag_path for tag_path in tag_script.getPaths()],
			'triggers': [t.toString() for t in tag_script.getChangeTypes()],
			'enabled': tag_script.isEnabled(),
			}, 2)
		
	
	def traverse_menu(parent_path, menu_node, mutable_dict):
		for mix, child in enumerate(menu_node.getChildren() or []):
			mutable_dict['%s/entry-%02d.py' % ('/'.join(parent_path), mix)] = child.getScript()
			mutable_dict['%s/entry-%02d.config.json' % ('/'.join(parent_path), mix)] = system.util.jsonEncode({
					'name': child.getName(),
					'icon': child.getIconPath(),
					'mnemonic': child.getMnemonic(),
					'description': child.getDescription(),
					'accelerator': child.getAccelerator(),
				}, 2)
			traverse_menu(parent_path + [child.getName() or ('Submenu-%02d' % mix)], child, mutable_dict)
	
	menu_root = client_script_config.getMenuRoot()
	traverse_menu(['menu'], menu_root, scripts)
	
	message_scripts = client_script_config.getMessageHandlerScripts()
	for message_script in message_scripts:
		scripts['message/%s.py' % message_script.getName()] = message_scripts[message_script]
		scripts['message/%s.config.json' % message_script.getName()] = system.util.jsonEncode({
				'name': message_script.getName(),
				'threading': str(message_script.getThreadType()),
				'enabled': message_script.isEnabled(),
			}, 2)
	
	return scripts


def extract_design_gatewayevents(resource_objects):
	assert len(resource_objects) == 1, 'Resource is expected to be contained in one root object'
	
	client_script_config = resource_objects[0]
	
	scripts = {}
	
	script = client_script_config.getStartupScript()
	if script:
		scripts['startup.py'] = script
	
	script = client_script_config.getShutdownScript()
	if script:
		scripts['shutdown.py'] = script

	timer_scripts = client_script_config.getTimerScripts()
	for timer_script in timer_scripts:
		scripts['timer/%s.py' % timer_script.getName()] = timer_scripts[timer_script]
		scripts['timer/%s.config.json' % timer_script.getName()] = system.util.jsonEncode({
			'enabled': timer_script.isEnabled(),
			'timing': 'delay' if timer_script.isFixedDelay() else 'rate',
			'period': timer_script.getDelay(),
			'threading': 'shared' if timer_script.isSharedThread() else 'dedicated',
			}, 2)
	
	for tag_script in client_script_config.getTagChangeScripts():
		scripts['tag-change/%s.py' % tag_script.getName()] = tag_script.getScript()
		scripts['tag-change/%s.config.json' % tag_script.getName()] = system.util.jsonEncode({
			'name': tag_script.getName(),
			'tags': [tag_path for tag_path in tag_script.getPaths()],
			'triggers': [t.toString() for t in tag_script.getChangeTypes()],
			'enabled': tag_script.isEnabled(),
			}, 2)
		
	message_scripts = client_script_config.getMessageHandlerScripts()
	for message_script in message_scripts:
		scripts['message/%s.py' % message_script.getName()] = message_scripts[message_script]
		scripts['message/%s.config.json' % message_script.getName()] = system.util.jsonEncode({
				'name': message_script.getName(),
				'threading': str(message_script.getThreadType()),
				'enabled': message_script.isEnabled(),
			}, 2)
	
	return scripts


def extract_design_webdev(resource_objects):
	assert len(resource_objects) == 1, 'Resource is expected to be contained in one root object'
	
	webdev = propsetToDict(resource_objects[0])
	
	scripts = {}
	config  = {}
	
	for attribute, value in webdev.items():
		if attribute.startswith('do'):
			scripts['.%s.py' % attribute[2:]] = value
		else:
			config[attribute] = value
	
	scripts['.config.json'] = system.util.jsonEncode(config, 2)
	
	return scripts



def extract_global_props(client_context):
	
	global_props = client_context.getGlobalProps()
	
	config = {
		'permissions': hashmapToDict(global_props.getPermissionEnabledMap()),
		'roles': {
			'client': dict((category, [role.strip() 
									   for role 
									   in role_string.split(',')
									   if role
									   ])
						   for category, role_string
						   in hashmapToDict(
						   		global_props.getRequiredClientRolesMap()
						   	).items()),
			'delete'  : [role.strip() for role in global_props.getRequiredDeleteRoles()],
			'publish' : [role.strip() for role in global_props.getRequiredPublishRoles()],
			'resource': [role.strip() for role in global_props.getRequiredResourceRoles()],
			'required': [role.strip() for role in global_props.getRequiredRoles()],
			'save'    : [role.strip() for role in global_props.getRequiredSaveRoles()],
			'view'    : [role.strip() for role in global_props.getRequiredViewRoles()],
			},
		'auditing': global_props.isAuditingEnabled(),
		'legacy': global_props.isLegacyProject(),
		'commitMessageMode': global_props.getCommitMessageMode().toString(), # enum
		'defaultSQLTagsProviderRate': global_props.getSqltagsClientPollRate(),
		}
	
	defaultable_attributes = set([
		'auditProfileName', 
		'authProfileName',
		'defaultDatasourceName', 
		'defaultSQLTagsProviderName',
		'publishMode',
		])
	
	for attribute in defaultable_attributes:
		try: # to get the Java getter first
		    # it's slightly more reliable than the Jython auto-attribute, in general
			getter_name = 'get' + attribute[0].upper() + attribute[1:]
			value = getattr(global_props, getter_name)()
		except AttributeError:
			try: # the Jython attribute
				value = getattr(global_props, attribute)
			except AttributeError:
				value = None
		
		if value is None:
			continue
		
		config[attribute] = value	
	
	return {
		'.config.json': system.util.jsonEncode(config, 2),
		}


def nop_dict(*args, **kwargs):
	return {}
	

type_dispatch = {
#	   'sr.script.shared': extract_global_script,
	  'sr.script.project': extract_project_script,

#	     'alarm-pipeline': extract_alarmpipeline,
#	        'named-query': extract_namedquery,


#	             'window': extract_window,
#	 'component-template': extract_template,
#	      'com.ia.report': extract_design_report,
#	              'group': extract_design_group,


#   'client.event.scripts': extract_design_clientevents,
#          'event.scripts': extract_design_gatewayevents,

#        'webdev-resource': extract_design_webdev,
 
#	        'client_tags': extract_design_client_tags,

#	'wsconsumerssettings': extract_global_wsconsumerssettings,
#	  'soapconfiguration': extract_global_soapconfiguration,
#	  'restconfiguration': extract_global_restconfiguration,
#	       'restendpoint': extract_global_restendpoint,

#	               'main': extract_global_main,
#
#	         'enterprise': extract_global_enterprise,
#	               'site': extract_global_site,
#	               'area': extract_global_area,
#	               'line': extract_global_line,
#	         'cell_group': extract_global_cell_group,
#	               'cell': extract_global_cell,
               
               '__folder': None,
	}


def extract_resources(resources, category='', context=context):
	"""Extract resource data. Category prepends to each resource's path"""
	extracted_data = {}
	
	for res_path, resource in resources.items():
		res_type = resource.getResourceType()
		extractor = type_dispatch.get(res_type, None)
		
		if not extractor:
			continue
		
		try:
			data_context = deserializer.deserializeBinary(resource.getData())
		except SerializationException, error:
			print 'Resource did not deserialize: %s\n%r (type: %s)' % (res_path, resource, res_type)
			print '    Err: %r' % error
			
		resource_objects = [obj for obj in data_context.getRootObjects()]
		
		dest_path, _, _ = res_path.rpartition('/')
		
		try:
			res_name = resource.getName()
			if res_name:
				dest_path += '/' + res_name
		except:
			pass

		if category:
			dest_path = category + '/' + dest_path
		
		extracted_data[dest_path] = extractor(resource_objects)
			
	return extracted_data


def dump_extracted_resources(destination_folder, extracted_data, purge_first=False):
	"""
	Dump the contents of the given extracted data into the destination folder.
	If purge_first is set True, then the destination will be deleted before dumping.
	"""
	if purge_first and os.path.exists(destination_folder):
		for subdir in os.listdir(destination_folder):
			if subdir.startswith('.'):
				continue
			
			try:
				shutil.rmtree(destination_folder + '/' + subdir)
			except OSError:
				print 'Destination folder not completely purged - check for open files!'
			
	for resource_path, resource_details in extracted_data.items():
		resource_path, _, name = resource_path.rpartition('/')
		
		destination = '%s/%s' % (destination_folder, resource_path)
				
		for suffix, data in resource_details.items():
			
			if suffix.startswith('.'):
				filepath = '%s/%s%s' % (destination, name, suffix)
			else:
				filepath = '%s/%s' % (destination, suffix)

			if data is None:
				print 'No data! %s' % filepath
				continue


			if not os.path.exists(filepath.rpartition('/')[0]):
				os.makedirs(filepath.rpartition('/')[0])
			
			with open(filepath, 'wb') as f:
				f.write(data)
	


