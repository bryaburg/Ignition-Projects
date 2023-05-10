from shared.Lodestar.R3.Config import projectName
from shared.Lodestar.R3.Util import clientScope


def insertSAPLogger(source='',type='',text='',severity=1):
	#inserts a value into the ScheduleLogger database table. 
	"""
	severity levels:
	1 = info
	2 = warning
	3+ = undefined
	"""
	args = {'SOURCE':source,
			'TYPE':type,
			'TEXT':text,
			'SEVERITY':severity
			}
	
	if clientScope():
		result = system.db.runNamedQuery('Scheduling/insertSAPLogger',args)
	else:
		result = system.db.runNamedQuery(projectName,'Scheduling/insertSAPLogger',args)
	
		return result

def insertSAPOrderChangeLog(orderNumber,columnName,orgVal,newVal,note):
	timestamp = system.date.now()
	
	args = {'ORDERNUMBER':orderNumber,
			'COLUMNNAME':columnName,
			'ORIGINALVALUE':orgVal,
			'NEWVALUE':newVal,
			'NOTE':note,
			'TIMESTAMP':timestamp
			}
	
	if clientScope():
		result = system.db.runNamedQuery('Scheduling/insertSAPOrderChangelog',args)
	else:
		result = system.db.runNamedQuery(projectName,'Scheduling/insertSAPOrderChangelog',args)
	
	return result
	
def insertSAPChangeLog(source,item,columnName,orgVal,newVal,note):
	timestamp = system.date.now()
	
	args = {'SOURCE':source,
			'ITEM':item,
			'COLUMNNAME':columnName,
			'ORIGINALVALUE':str(orgVal),
			'NEWVALUE':str(newVal),
			'NOTE':note,
			'TIMESTAMP':timestamp
			}
	
	if clientScope():
		result = system.db.runNamedQuery('Scheduling/insertSAPChangelog',args)
	else:	
		result = system.db.runNamedQuery(projectName,'Scheduling/insertSAPChangelog',args)
	
	return result
	
def insertScheduleLogger(source='',type='',text='',severity=1):
	#inserts a value into the ScheduleLogger database table. 
	"""
	severity levels:
	1 = info
	2 = warning
	3+ = undefined
	"""
	args = {'source':source,
			'type':type,
			'text':text,
			'severity':severity
			}
	
	if clientScope():
		result = system.db.runNamedQuery('Scheduling/insertScheduleLogger',args)
	else:	
		result = system.db.runNamedQuery(projectName,'Scheduling/insertScheduleLogger',args)
	
	return result