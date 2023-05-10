from uuid import UUID, uuid4
from collections import defaultdict
from stratum.types import *
from stratum.metaproperty import MetaProperty
from stratum.templatedefiner import TemplateDefiner
from stratum.template import Template
from stratum.sync.bulk import fully_precache_instances, fully_precache_templates
Type = TemplateDefiner

import xml.etree.ElementTree as ET
	
def importReasonCodes():
	filePath = system.file.openFile('xml')
	#print filePath
	#filePath = r'/home/toor/NoWCMStatesLN07.xml'
	tree = ET.parse(filePath)
	root = tree.getroot()
	
	states = root.find('EquipmentStateClass')
	#states = states.find('EquipmentState')
	
	
	def getLeafs(elem):
		leafs = elem.findall('EquipmentState')
		if leafs:
			ret = []
			for x in leafs:
				res = getLeafs(x)
				ret = ret + res
			return ret
		
		else:
			name = elem.find('Name').text
			code = elem.find('Code').text
			typ = elem.find('Type').text == 'Unplanned Downtime'
			
			return[[name,code,typ]]
			print name,code,typ
		
	tmp = getLeafs(states)
	
	fully_precache_templates()
	fully_precache_instances()
	
	reasonCodes = {}
	
	for (name,code,typ) in tmp:
		print '%70s%10s%10s' % (name,code,typ)
		reasonCode[name] = Type['reasonCode'](name,code,typ)
