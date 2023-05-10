'''
	Station Screen Scripts
	Created:  JGV 2019-11-07
		
	Updated by:	JGV 2020-03-06 - Split off shared functions for web service support.
				WJF 2020-03-12 - Collecting more functions for web service support.
					2021-04-27 - Updated autoScannerNewValue to support extraneous info
								 after 'model,serial'.
					2021-04-28 - Created getModelSerialFromTag.
					2021-05-06 - Moved more logic into getModelSerialFromTag.
					2021-06-21 - Added getVicinityImages().
					2021-07-06 - autoScannerNewValue() will not fire on initialChange.
							   - hacksaw() fail will only report the raw string.
					2021-07-07 - getListOfUnitDefects() now grabs the ComponentLocationName.
'''


import re
from shared.Common.Util import getUser
from shared.RTY.General import getRTYDb, getLodestarDb, insertLog
from shared.lodestar_core.utilities import hacksaw


def validModelCheck(ModelName):
	'''
		Returns the ModelId or None.
	'''
	query = """
			SELECT M.ModelId
			FROM Model M
			WHERE M.ModelName = ?
			"""
	return system.db.runScalarPrepQuery(query, [ModelName], getRTYDb())
		
		
def validPlatformCheck(subPlatformName):
	'''
		Returns the SubPlatformId or None.
	'''
	query = """
			SELECT SubPlatformId
			FROM SubPlatform
			WHERE SubPlatformName = ?
			"""
	return system.db.runScalarPrepQuery(query, [subPlatformName], getRTYDb())


def validSerialCheck(serialNumber):
	'''
		Checks length and content of string against Whirlpool standards.
		[Letter location][Letter or number year][Week number][5 number code]
		(as of 2020-02-14)
	'''
	regex = system.tag.read('[Configuration]RTY/Serial_Number/Regular_Expression').value
	serialNumRegex = re.compile(r"%s" % regex)
	if serialNumRegex.search(serialNumber):
		return True
	
	return False
	
	
def validToteCheck(serialNumberName):
	'''
		Returns True for now.
	'''
	return True


def checkSerialNumberExist(serialNumber, modelId=None):
	'''
		Takes serialNumber and modelId.
		Returns serialNumberId or None.
	'''
	query = """
			SELECT SerialNumberId
			FROM dbo.SerialNumber
			WHERE SerialNumberName = ?
			"""
	queryParams = [serialNumber]
	
	if modelId:
		query += " AND ModelId = ? "
		queryParams.append(modelId)
	else:
		query += " AND ModelId IS NULL"
	
	return system.db.runScalarPrepQuery(query, queryParams, getRTYDb())


def insertSerialNumber(modelId, serialNumberName, modelNumber):
	'''
		Inserts a serial number record into the table.
		Returns the id of the record.
	'''
	query = """
			INSERT INTO dbo.SerialNumber
				(ModelId, SerialNumberName, ModelNumber)
			VALUES
			"""
	if modelId and modelNumber:
		query += "(?, ?, ?)"
		queryParams = [modelId, serialNumberName, modelNumber]
	else:
		query += "(NULL, ?, ?)"
		queryParams = [serialNumberName, 'TOTE']
	
	return system.db.runPrepUpdate(query, queryParams, getRTYDb(), getKey=1)


def validBadgeCheck(badgeNumber):
	'''
		Checks length and content of string against RegEx in the tag.
	'''
	regex = system.tag.read('[Configuration]RTY/Badge_Number/Regular_Expression').value
	badgeNumRegex = re.compile(r"%s" % regex)
	if badgeNumRegex.search(badgeNumber):
		return True
	
	return False


def findAuditor(badgeNumber):
	'''
		Looks for Badge in Auditor table.
		Returns AuditorID if found.
		Returns None if not found.
	'''
	query = """
			SELECT AuditorId 
			FROM Auditor 
			WHERE AuditorBadgeNumber = ?
			"""
	return system.db.runScalarPrepQuery(query, [badgeNumber], getRTYDb())
		
	
def createAuditor(badgeNumber):
	'''
		Create badge number in auditor table.
	'''
	query = """
			INSERT INTO Auditor(AuditorBadgeNumber) 
			VALUES(?)
			"""
	return system.db.runPrepUpdate(query, [badgeNumber], getRTYDb(), getKey=1)

	
def getNavigationButtonParams(vicinityLocations):
	'''
		We passed in the vicinity locations so the property change mechanism would fire the event
	'''
	headers = ['btnId', 'btnName']
	rows = []
	print  "getNavigationButtonParams", vicinityLocations.rowCount, len(system.dataset.getColumnHeaders(vicinityLocations)), system.dataset.getColumnHeaders(vicinityLocations)
	for rowIndex in range(vicinityLocations.rowCount):
		rows.append([vicinityLocations.getValueAt(rowIndex, 'ComponentLocationId'), vicinityLocations.getValueAt(rowIndex, 'ComponentLocationName')])
		print "getNavigationButtonParams", vicinityLocations.getValueAt(rowIndex, 'ComponentLocationId'), vicinityLocations.getValueAt(rowIndex, 'ComponentLocationName') \
				, vicinityLocations.getValueAt(rowIndex, 'ImageId'), vicinityLocations.getValueAt(rowIndex, 'AllowZoomIn') \
				, vicinityLocations.getValueAt(rowIndex, 'ImageDisplayFilePath'), vicinityLocations.getValueAt(rowIndex, 'ImageMapFilePath')
	return system.dataset.toDataSet(headers, rows)	


def getChildLocationIds(vicinityLocations):
	'''
		We passed in the vicinity locations so the property change mechanism would fire the event
	'''
	headers = ['componentLocationId']
	rows = []
	print  "getChildLocationIds", vicinityLocations.rowCount, len(system.dataset.getColumnHeaders(vicinityLocations)), system.dataset.getColumnHeaders(vicinityLocations)
	for rowIndex in range(vicinityLocations.rowCount):
		rows.append([vicinityLocations.getValueAt(rowIndex, 'ComponentLocationId')])
		print "getChildLocationIds", vicinityLocations.getValueAt(rowIndex, 'ComponentLocationId'), vicinityLocations.getValueAt(rowIndex, 'ComponentLocationName') \
				, vicinityLocations.getValueAt(rowIndex, 'ImageId'), vicinityLocations.getValueAt(rowIndex, 'AllowZoomIn') \
				, vicinityLocations.getValueAt(rowIndex, 'ImageDisplayFilePath'), vicinityLocations.getValueAt(rowIndex, 'ImageMapFilePath')
	return system.dataset.toDataSet(headers, rows)	


def getVicinityPath(vicinityStack):
	path = ''
	for rowIndex in reversed(range(vicinityStack.rowCount)):
		path += vicinityStack.getValueAt(rowIndex, "ComponentLocationName")
		if rowIndex:
			path += "/"
	return path


def getVicinityImages(vicinityId):
	'''Returns a dataset of vicinityID's image data.
		
		Args:
			vicinityID (int): Component Location Id
			
		Returns:
			pyDataSet: Contains display image and image map locations.
	'''
	query = '''
			SELECT [ImageDisplayFilePath]
					,[ImageMapFilePath]
			FROM  [dbo].[vw_ComponentLocation]
			WHERE ComponentLocationId = ?
			'''
	return system.db.runPrepQuery(query, [vicinityId], getRTYDb())


def getUnitCountForStationAndShift(throwAwayVariableToBindARefresh):
	'''
		DocString!
	'''
	stationId = system.tag.read('[Client]Station/Station_Id').value
	
	query = """
			SELECT COUNT(1)
			FROM InspectionResult IR
				LEFT JOIN Station S
					ON IR.StationId = S.StationId
				LEFT JOIN LINE L
					ON S.LineName = L.WhirlpoolLineCode
			WHERE IR.StationId = ?
				AND InspectionResultTimestamp BETWEEN dbo.fn_GetCurrentShiftStartTime(L.WhirlpoolLineCode) AND GETDATE()
			"""
	return system.db.runScalarPrepQuery(query, [stationId], getRTYDb())     

def getListOfUnitDefects(serialNumberId, modelNumberId):
	'''
		DocStrings!
	'''
	if serialNumberId:
		query = """
				SELECT D.DefectId
						,D.DefectCodeName
						,D.DefectDetailName
						,CL.ComponentLocationName AS ComponentName
						,D.DefectTimestamp
						,D.StatusID
						,D.Comment
						,D.RepairComment
						,D.Deleted
				FROM Defect D
					INNER JOIN InspectionResult I
						ON D.InspectionResultId = I.InspectionResultId
					INNER JOIN SerialNumber SN
						ON I.SerialNumberId = SN.SerialNumberId
					LEFT JOIN ComponentLocation CL
						ON D.ComponentLocationId = CL.ComponentLocationId
				WHERE SN.SerialNumberId = ?
				"""
		queryParams = [serialNumberId]
		
		if modelNumberId:
			query += " AND SN.ModelId = ? "
			queryParams.append(modelNumberId)
			
		return system.db.runPrepQuery(query, queryParams, getRTYDb())
	
	return system.dataset.toDataSet([], [])


def markDefectRepaired(auditor, defectId):
	'''
		Marks the selected Defect as repaired and notes the auditor.
	'''
	query = """
			UPDATE Defect
			SET StatusId = 2
				,RepairedByBadgeNumber = ?
				--,RepairComment = ?
				--,UpdatedBy = ''
				--,UpdatedOn = GETDATE()
			WHERE DefectId = ?
			"""		
	system.db.runPrepUpdate(query, [auditor, defectId], getRTYDb())


def markDefectDeleted(defectId):
	'''
		Marks the selected Defect as deleted.
		NOTE:  We may want to swap this out with Flip Deleted Flag.
	'''
	query = """
			UPDATE [dbo].[Defect]
			SET Deleted = 1
				--,UpdatedBy = ''
				--,UpdatedOn = GETDATE()
			WHERE DefectId = ?
			"""
	system.db.runPrepUpdate(query, [int(defectId)], getRTYDb())



########## Inspection Result Scripts ##################################


def getDefectBasicsForSerialNumber(modelName, serialName):
	'''
		Takes the raw model/serial text.
		Returns a dataset of the Id's for the associated defect(s).
	'''
	query = """
			SELECT D.DefectId AS defectId, D.ComponentLocationId as componentLocationId, D.DefectCodeId AS defectCodeId, D.DefectDetailId AS defectDetailId
			FROM SerialNumber SN
				JOIN InspectionResult IR
					ON SN.SerialNumberId = IR.SerialNumberId
				JOIN Defect D
					ON IR.InspectionResultId = D.InspectionResultId
			WHERE ModelNumber = ?
				AND SerialNumberName = ?
			"""
	return system.db.runPrepQuery(query, [modelName, serialName], getRTYDb()).underlyingDataset


def lookupInspectionResultId(serialNumberId, platformId, stationId, auditorId):
	'''
		Returns InspectionResultId if it exists with the given parameters.
		Returns None if it does not exist.
	'''
	query = """
			SELECT InspectionResultId 
			FROM [dbo].[InspectionResult]
			WHERE SerialNumberId = ?
				AND PlatformId = ?
				AND StationId = ?
				AND AuditorId = ?
			"""
	return system.db.runScalarPrepQuery(query, [serialNumberId, platformId, stationId, auditorId], getRTYDb())


def updateInspectionResultTimestamp(inspectionId):
	'''
		Updates timestamp of given inspection Id.
	'''
	query = """
			UPDATE InspectionResult
			SET InspectionResultTimestamp = SYSDATETIME()
			WHERE InspectionResultId = ?
			"""
	system.db.runPrepUpdate(query, [inspectionId], getRTYDb())


def insertNewInspectionRecord(serialNumberId, platformId, stationId, auditorId):
	'''
		Inserts new inspection record and returns the id value of that record.
	'''
	query = """
			INSERT INTO InspectionResult(SerialNumberId, PlatformId, StationId, AuditorId, InspectionResultTimestamp)
			VALUES(?, ?, ?, ?, SYSDATETIME())
			"""
	return system.db.runPrepUpdate(query, [serialNumberId, platformId, stationId, auditorId], getRTYDb(), getKey=1)


def getSubPlaformNameId(newModelText):
	'''
		Returns pyDataset of subPlatformID and subPlatformName from given (valid) Model.
	'''
	query = """
			SELECT SP.SubPlatformId, SP.SubPlatformName
			FROM SubPlatform SP
				JOIN Model M
					ON SP.SubPlatformId = M.SubPlatformId
					AND M.ModelName = ?
			"""
	return system.db.runPrepQuery(query, [newModelText], getRTYDb())


def getComponentInfo(platformId, stationId): # check for similar code elsewhere
	'''
		Get component location name and id from platform and station ids.
	'''
	query = """
			SELECT ISNULL(cl.[ComponentLocationId], 0) [ComponentLocationId]
					,ISNULL(cl.[ComponentLocationName], '') [ComponentLocationName]
			FROM Station s
				LEFT JOIN StationAssignment sa
					ON sa.SubPlatformId = ?
						AND sa.StationId = s.StationId
						AND sa.Deleted = 0
				LEFT JOIN ComponentLocation cl
					ON sa.ComponentLocationId = cl.ComponentLocationId
			WHERE s.StationId = ?
			"""
	return system.db.runPrepQuery(query, [platformId, stationId], getRTYDb())


def getLocationList(platformId, vicinityId):
	'''
		# Init the location list
	'''
	query = """
			SELECT ISNULL(cl.ComponentLocationId, 0) [ComponentLocationId]
					,cl.ComponentLocationName
					,MAX(cl.ImageId) [ImageId]
					,MAX(cl.ImageDisplayFilePath) [ImageDisplayFilePath]
					,MAX(cl.ImageMapFilePath) [ImageMapFilePath]
					,MAX(cl.Color) [Color]
					,MAX(cl.ComponentName) [ComponentName]
					,CASE WHEN COUNT(children.ChildLocationId) > 0 THEN 1 ELSE 0 END [AllowZoomIn]
			FROM vw_ComponentLocation cl
				LEFT JOIN ComponentLocationMap children
					ON cl.ComponentLocationId = children.ParentLocationId
				LEFT JOIN ComponentLocationMap parent
					ON cl.ComponentLocationId = parent.ChildLocationId
			WHERE cl.SubPlatformId = %d
				AND parent.ParentLocationId = %d
			GROUP BY cl.ComponentLocationId, cl.ComponentLocationName
			ORDER BY cl.ComponentLocationName
			"""%(platformId, vicinityId)
	return system.db.runQuery(query, getRTYDb())


def getLocationTreeTop(platformId):
	'''
		Returns top of the ComponentLocation Tree for given Platform.
	'''
	query = """
			SELECT ISNULL(cl.ComponentLocationId, 0) AS [ComponentLocationId]
					,cl.ComponentLocationName
					,MAX(cl.ImageId) AS [ImageId]
					,MAX(cl.ImageDisplayFilePath) AS [ImageDisplayFilePath]
					,MAX(cl.ImageMapFilePath) AS [ImageMapFilePath]
					,MAX(cl.Color) AS [Color]
					,MAX(cl.ComponentName) AS [ComponentName]
					,CASE WHEN COUNT(children.ChildLocationId) > 0 THEN 1 ELSE 0 END AS [AllowZoomIn]
			FROM vw_ComponentLocation cl
				LEFT JOIN ComponentLocationMap children
					ON cl.ComponentLocationId = children.ParentLocationId
				LEFT JOIN ComponentLocationMap parent
					ON cl.ComponentLocationId = parent.ChildLocationId
			WHERE cl.SubPlatformId = ?
				AND parent.ParentLocationId IS NULL
			GROUP BY cl.ComponentLocationId, cl.ComponentLocationName
			ORDER BY cl.ComponentLocationName
			"""
	return system.db.runPrepQuery(query, [platformId], getRTYDb())


def getDisplayAndMap(vicinityId):
	'''
		Return Image and ImageMap from ComponentLocation.
	'''
	query = """
			SELECT ImageDisplayFilePath
					,ImageMapFilePath 
			FROM vw_ComponentLocation cl
			WHERE cl.ComponentLocationId = ?
			"""
	return system.db.runPrepQuery(query, [vicinityId], getRTYDb())


def getDefectLocationData(inspectionId, imageID):
	'''
		Return Defect Names, locations, and status from InspectionResult and Image.
	'''
	query = """
			SELECT DefectDetailName
					,ImageCoordinateX
					,ImageCoordinateY
					,StatusID 
			FROM Defect 
			WHERE InspectionResultId = ? 
				AND ImageId = ?
			"""
	return system.db.runPrepQuery(query,[inspectionId, imageID], getRTYDb())


################### Auto Scan/Timer/Button ###############################


def getAutoScanTimeout(stationTypeName):
	'''
		Gets AutoScan time limit based on stationTypeName.
	'''
	query = """
			SELECT ISNULL(AutoScanTimer, 0)
			FROM StationType
			WHERE StationTypeName = ?
			"""
	return system.db.runScalarPrepQuery(query, [stationTypeName], getRTYDb())


def autoScannerNewValue(tagPath, previousValue, currentValue, initialChange, missedEvents):
	'''Tag value is stripped, validated, and passed to tags.   New data tag is set.
	
		Triggered when a new value arrives from the PLC.
		Writes Serial string into the Serial tag, Model string into the Model tag,
		and flags the front end that new data has arrived by setting the New Data tag to True.
		
		Args:
			tagPath (str): String of the path where the tag that called this resides.
			previousValue (qualified value): Previous value of the tag that called this.
			currentValue (qualified value): Current value of the tag that called this.
			initialChange (bool): Is this change the first change after the system was turned on?
			missedEvents (bool): Are there any events that the system was not able to send to this function?
	'''
	if not initialChange:	
		functionName = tagPath
		user = getUser(None)
		firstNonModelChar = system.tag.read('[Configuration]RTY/Serial_Number/Serial_Beginning_String').value
		firstNonSerialChar = system.tag.read('[Configuration]RTY/Serial_Number/Serial_Ending_String').value
		
		model, serial = getModelSerialFromTag(currentValue.value,
												firstNonModelChar,
												firstNonSerialChar,
												functionName,
												user)
		
		system.tag.write('[.]Serial', serial)
		system.tag.write('[.]Model', model)
		system.tag.write('[.]New_Data', True)


def getModelSerialFromTag(raw, modelSplit, serialSplit, functionName='Test', user='Test'):
	'''Given a raw string, splits it on the Splits, and returns model/serial strings.
	 	
	 	Splits the string into model and serial numbers. Dumps the rest.
		Validates the model.  If valid, grabs the modelId.  Failures get a false ID (-1).
		Validates the Serial/Model pair.  If valid, grabs the serialId.
		Failures it validate the Serial.  If valid, attempts to insert it in the table.
		Inserts will fail on a non-existent modelId due to Foriegn Key requirements.
	 	
	 	NOTE:  Errors in this code are written to the INTERNAL RTY logger (in SQL.dbo.Logger).
		NOTE:  The Serial Ending String should match the end-of-line code for the barcode scanners.
		NOTE:  Good models in SAP will fail if the program has not been updated from SAP.
		
		Args:
			raw (str): Of the form, <model><modelSplit><serial><serialSplit><garbage>
			modelSplit (str): Demarks the end of the model and start of the serial.
			serialSplit (str): Demarks the end of the serial and start of the garbage.
			functionName (str): Name of the tag path that called the function. Used in logging.
			user (str): Name of the user who was logged into Ignition. Used in logging.
		
		Returns:
			model (str): Model Number
			serial (str): Serial Number
	'''
	try:
		model, serial = hacksaw(raw, [modelSplit, serialSplit], 1)[:2]
		
	except Exception, error:
		logType = 'Bad Data from Line Scan'
		log = ('Current value failed to split on special characters.'
			+ '  Raw Tag Value = ' + str(raw)
			+ '  Error = ' + str(error))
		insertLog(functionName, logType, log, user)
		return None, None
		
	else: #sanity checks model/serial
		modelId = validModelCheck(model)
		if not modelId:
			logType = 'Bad Model from Line Scan'
			log = ('Model failed validity check.'
				+ '  Current Tag Value = ' + raw 
				+ ', Model = ' + model 
				+ ', Serial = ' + serial)
			insertLog(functionName, logType, log, user)
		
		else:
			serialNumberId = checkSerialNumberExist(serial, modelId)
			if not serialNumberId: #if model/serial does not already exist in the database
				if not validSerialCheck(serial):
					logType = 'Bad Serial from Line Scan'
					log = ('Serial failed validity check.'
							+ '  Current Tag Value = ' + raw
							+ ', Serial = ' + serial 
							+ ', SerialNumberId = ' + str(serialNumberId))
					insertLog(functionName, logType, log, user)
				try:
					insertSerialNumber(modelId, serial, model) #will fail on modelId = -1 due to Foreign Key error
				except:
					logType = 'Insert Serial Number Failed'
					log = ('Check uniqueness of modelId, serial, model.'
							+ '  Current Tag Value = ' + raw
							+ ', modelId = ' + str(modelId) 
							+ ', Serial = ' + serial 
							+ ', model = ' + model
							+ '  Error = ' + str(error))
					insertLog(functionName, logType, log, user)
		return model, serial


def stationUnitCount(stationId, currentShiftStartTime):
	'''
		How many units a given station has ran on a given shift.
	'''
	query = """
			SELECT count(distinct(SerialNumberName))
			FROM [dbo].[vw_InspectionResultDetails]
			WHERE StationId = ?
				AND InspectionResultTimestamp BETWEEN ? AND getdate()
			"""
	return system.db.runScalarPrepQuery(query, [stationId, currentShiftStartTime], getRTYDb())
