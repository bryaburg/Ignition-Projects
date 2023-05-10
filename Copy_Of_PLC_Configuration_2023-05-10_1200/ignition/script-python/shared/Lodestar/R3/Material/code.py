from shared.Lodestar.R3.Config import projectName
from shared.Lodestar.R3.Util import clientScope


# SAP Material Groups 
# FERT = Finished good
# HALB = Semifinished
# ROH = Raw
# ZHAL = Engineering Model

def checkConfigureMat(materialClass, materialName, linePath, reConfigure=False):
	classLink = checkCreateMaterialClass(materialClass)
	matLink = checkCreateMaterialDef(materialName, classLink)
	checkCreateMaterialProcessSegment(matLink, materialName, linePath, reConfigure)
	
	return matLink
	
def checkCreateMaterialClass(name):
	#This function is passed a name for a Material Class and will create it if it doesn't already exist.
	#This class will be added to the Material Root, allowing any material definitions in this class to
	#be used by The OEE Material Manager
	
	filter = system.mes.object.filter.createFilter()
	filter.setMESObjectTypeName('MaterialClass')
	filter.setMESObjectNamePattern(name)
	list = system.mes.searchMESObjects(filter)
	if list.size() == 0:
		obj = system.mes.createMESObject('MaterialClass')
		obj.setPropertyValue('Name', name)
		link = system.mes.object.link.create(obj)
	else:
		link = list.get(0)
		obj = list.get(0).getMESObject()     
		 
	rootFolder = 'Material Root'
	parentList = obj.getParentCollection().getList()
	if parentList.findByName(rootFolder) is None:      
		# add to root
		obj.addParent(system.mes.loadMESObject(rootFolder, 'MaterialRoot'))
	system.mes.saveMESObject(obj)
	   
	return link
    
def checkCreateMaterialDef(name, objClassLink):
	#This function will check if the passed Material definition exists and if not creates it.
	#It will then check to see if it is a member of the given Material class and if not will add it.
	
	filter = system.mes.object.filter.createFilter()
	filter.setMESObjectTypeName('MaterialDef')
	filter.setMESObjectNamePattern(name)
	list = system.mes.searchMESObjects(filter)
	if list.size() == 0:
		obj = system.mes.createMESObject('MaterialDef')
		obj.setPropertyValue('Name', name)
	else:
		obj = list.get(0).getMESObject()
		 
	if objClassLink is not None:
		clsName = objClassLink.getName()
		parentList = obj.getParentCollection().getList()
		if parentList.findByName(clsName) is None:             
			obj.addParent(objClassLink)
	system.mes.saveMESObject(obj)
	objLink = system.mes.object.link.create(obj)

	return objLink
	
def checkCreateMaterialProcessSegment(matLink, materialName, linePath, reConfigure):
	#Given the line and material, we can ensure the OEE Run related operation is present:
	#If it is not present, we must create the changeover and production Operations.

	operList = system.mes.oee.getMaterialOperationsSegments(matLink, '*' + linePath.split('\\')[-1] + '*')
	if operList.size() < 1:
		newOpList = system.mes.oee.createMaterialProcessSegment(matLink, linePath)
		
		opDef = newOpList.get(0)
		configureOpDef(opDef)
		
		coSeg = newOpList.get(1)
		prodModeUUID = getProductionModeUUID(linePath)
		configureCOSeg(coSeg, prodModeUUID)
		
		prodSeg = newOpList.get(2)
		configureProdSeg(prodSeg)
		
		system.mes.saveMESObjects(newOpList)
	elif reConfigure:
		
		opDef = getOpDef(materialName, linePath)
		configureOpDef(opDef)
		system.mes.saveMESObject(opDef)
		
		prodSeg = operList.get(0)
		configureProdSeg(prodSeg)
		
		coSeg = operList.get(1)
		prodModeUUID = getProductionModeUUID(linePath)
		configureCOSeg(coSeg, prodModeUUID)
		
		system.mes.saveMESObjects(operList)
		
def configureOpDef(opDef):
	
	triggerName = 'DefaultBeginTrigger'
	
	if triggerName not in opDef.getComplexPropertyItemNames('TriggerOperBegin'):
		#if the propperty doesn't exist, we create it
		begTrig = opDef.createComplexProperty('TriggerOperBegin', 'DefaultBeginTrigger')
	else:
		#if the propperty exists, we load it
		begTrig = opDef.getComplexProperty('TriggerOperBegin', 'DefaultBeginTrigger')
	
	begTrig.setMode('Schedule (time)')
	begTrig.setAuto(False)
	
	opDef.setUpdateEventInterval(10)
	
def configureCOSeg(coSeg, prodModeUUID):
	
	beginTrig = coSeg.getComplexProperty('TriggerSegBegin', 0)
	beginTrig.setAuto(True)
	
	endTrig = coSeg.getPrimaryEndTrigger()
	endTrig.setMode('Fixed Duration')
	endTrig.setFixedDuration(1)

	# set changeover mode to Production to accrue production counts during operation
	productionSettings = coSeg.getComplexProperty('ProductionSettings', 0)
	productionSettings.setModeRefUUID(prodModeUUID)
	
def configureProdSeg(prodSeg):
	
	ProdTrigger = prodSeg.getComplexProperty('TriggerSegEnd', 0)
	ProdTrigger.setMode('Schedule (production)')
	ProdTrigger.setAuto(False)
	
def getMaterialDef(materialName):
	#if material found, gets MES object for material
	filter = system.mes.object.filter.createFilter()
	filter.setMESObjectTypeName('MaterialDef')
	filter.setMESObjectNamePattern(materialName)
	list = system.mes.searchMESObjects(filter)
	if list.size() == 0:
		return None
	else:
		return list.get(0).getMESObject()

def getOpDef(materialName, linePath):
	searchString = materialName + '-' + linePath.replace('\\', ':')
	operList = system.mes.getAvailableOperations(linePath, searchString, True, True)
	operDef = operList.get(0).getMESObject()
	return operDef
	
def getMaterialDescription(material):
	params = {'material': material}
	if clientScope():
		description = system.db.runNamedQuery('Scheduling/getMaterialDescription', params)
	else:
		description = system.db.runNamedQuery(projectName, 'Scheduling/getMaterialDescription', params)
	return description
	

	
def getProductionModeUUID(linePath):
	productionMode = system.mes.getEquipmentModeOptions(linePath, 'Production').get(0)
	return productionMode.getUUID()
	
def getMaterialBOM(plant,material):
	spCall = system.db.createSProcCall('ReadSAPBOMTreeItemsByPN','IgnitionMES_Extension')
	spCall.registerInParam('plant',system.db.VARCHAR,plant)
	spCall.registerInParam('part_num',system.db.VARCHAR,material)

	system.db.execSProcCall(spCall)

	results = spCall.getResultSet()
	return results
	
def getEngModel(prodLine, material, altBOM):
	params = {'prodLine': prodLine, 'material': material, 'alternative': altBOM}
	if clientScope():
		engModel = system.db.runNamedQuery('Scheduling/getEngModel', params)
	else:
		engModel = system.db.runNamedQuery(projectName, 'Scheduling/getEngModel', params)
	return engModel
		
def getSalesModel(engModel):
	queryName = 'Scheduling/getSalesModel'
	params = {'engModel': engModel}
	if clientScope():
		salesModel = system.db.runNamedQuery(queryName, params)
	else:
		salesModel = system.db.runNamedQuery(projectName, queryName, params)
	return salesModel
	
def plcModelManipulation(engModel):
	kenmorePrefix = system.tag.readBlocking("R3/Config/Kenmore Prefix")[0].value
	kenmoreLetters = system.tag.readBlocking("R3/Config/Kenmore Letters")[0].value
	if engModel[:2] == kenmorePrefix and engModel[-4] in kenmoreLetters:
		engModel = engModel[2:].replace(engModel[-4],'')
	return engModel