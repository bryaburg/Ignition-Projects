from shared.Lodestar.R3.Util import clientScope

servers = { 'awn-mesappd1':'NED',
			'awn-mesappq1':'NEQ',
			'cly-mesappq3':'NPD',
			'cly-mesappp1':'NPD'
		}

ignPrimaryDB = 'IgnitionMES'
sapDB = 'IgnitionMES_Extension'

gatewayName = system.tag.readBlocking('R3/Config/GatewayName')[0].value
plantCode = system.tag.readBlocking('R3/Config/Plant Code')[0].value
plantName = system.tag.readBlocking('R3/Config/Plant Name')[0].value
siteName = system.tag.readBlocking('R3/Config/Site Name')[0].value
plantSerialCode = system.tag.readBlocking('R3/Config/Serial Plant Code')[0].value
WM = system.tag.readBlocking('R3/Config/Warehouse')[0].value

projectName = system.tag.readBlocking('R3/Config/ProjectName')[0].value

activeFGDs = system.tag.readBlocking('R3/Config/Active FG Lines')[0].value
activeFGLines = shared.Lodestar.R3.Util.dsColToList(activeFGDs, 'Line')

def getProjectName():
	return projectName
	
def getGatewayName():
	return gatewayName

def getPlantCode():
	return plantCode

def getWM():
	return WM
	
def getPlantSerialCode():
	return plantSerialCode
	
def getPlantName():
	return plantName
			
def getSiteName():
	return siteName
	
def getPrimarySAPSource():
	return system.tag.readBlocking('SAP/Source/Primary')[0].value

def getActiveSAPSource():
	return system.tag.readBlocking('SAP/Source/Active')[0].value
	
def getLinePath(line):
	siteName = system.tag.readBlocking('[R3]R3/Config/Site Name')[0].value
	return 'Whirlpool MES/%s/Assembly/%s' %(siteName, line)
								
#def getLinePath(line):
#	lineObj = system.mes.loadMESObject(line, 'Line')
#	linePath = lineObj.getEquipmentPath()
#	return linePath
	
def getLineID(line):
	params = {'line': line}
	if clientScope():
		id = system.db.runNamedQuery('Scheduling/getLineID', params)
	else:
		id = system.db.runNamedQuery(projectName, 'Scheduling/getLineID', params)
	return id