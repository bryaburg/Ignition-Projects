'''Designed to hold Reports:Defect Summary specific scripts

	Created:	JGV - 03-06-2020 - Split off shared functions for web services.
	Updated:	WJF - 2021-05-24 - Added (yet another) parameter to getPerformanceData.
				WJF - 2021-08-05 - Added groups to getPerformanceData().
'''


from shared.RTY.General import insertLog, getRTYDb, datesSelectedAndInRange
from shared.Common.Util import getUser
from shared.RTY.DefectEntry.DefectEntry import getDefectData
from shared.RTY.Reports.TopDefects import ddComponentLocation, ddNonconformity, ddResponsibility, ddDefect


def buildWhereClause(filters={}):
	'''<short description>
								
		<long description>
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
	filterVariables = []
	
	if filters['startDate'] and filters['endDate']:
		formatForSQL = 'yyyy-MM-dd HH:mm:ss'
		startDateFormat = system.date.format(filters['startDate'], formatForSQL)
		endDateFormat = system.date.format(filters['endDate'], formatForSQL)
		
		dateClause = """ AND (UpdatedOn BETWEEN ? AND ? 
							OR CreatedOn BETWEEN ? AND ?) """
		filterVariables.append(startDateFormat)
		filterVariables.append(endDateFormat)
		filterVariables.append(startDateFormat)
		filterVariables.append(endDateFormat)
	else:
		dateClause = ""
		
	if filters['lineName'] not in ['All', 'Todo']:
		lineClause = " AND LineName = ?"
		filterVariables.append(filters['lineName'])
	else:
		lineClause = ""
		
	if filters['shift']:
		shiftClause = " AND Shift = ?"
		filterVariables.append(filters['shift'])
	else:
		shiftClause = ""

	if filters['stationId']:
		stationClause = " AND StationId = ?"
		filterVariables.append(filters['stationId'])
	else:
		stationClause = ""
		
	if filters['platformName'] not in ['All', 'Todo']:
		platformClause = " AND SubPlatformName = ?"
		filterVariables.append(filters['platformName'])
	else:
		platformClause = ""

	if filters['componentLocationName'] not in ['All', 'Todo']:
		componentLocationClause = " AND ComponentLocationName = ?"
		filterVariables.append(filters['componentLocationName'])
	else:
		componentLocationClause = ""
	
	if filters['defectName'] not in ['All', 'Todo']:
		defectClause = " AND DefectDetailName = ?"
		filterVariables.append(filters['defectName'])
	else:
		defectClause = ""
	
	if filters['groupName'] not in ['All', 'Todo']:
		groupClause = " AND dbo.fn_GetGroup(LineCode, InspectionResultTimestamp) = ? "
		filterVariables.append(filters['groupName'])
	else:
		groupClause = ""
	
	whereClause = 'WHERE 1=1 ' + dateClause + lineClause + shiftClause +  stationClause + platformClause + componentLocationClause + defectClause + groupClause
	return whereClause, filterVariables


def getPerformanceData(lineName='All', shift=None, stationId=None, platformName='All', componentLocationName='All', defectName='All',
						startDate=None, endDate=None, timeDimension=-1, groupName='All'):
	'''<short description>
								
		<long description>
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
	if not datesSelectedAndInRange(timeDimension, [startDate, endDate]):
		return system.dataset.toDataSet([],[])  #return a blank dataset
	
	filters = {'lineName': lineName,
				'shift': shift,
				'stationId': stationId,
				'platformName': platformName,
				'componentLocationName': componentLocationName,
				'defectName': defectName,
				'startDate': startDate,
				'endDate': endDate,
				'groupName': groupName}

	whereClause, queryParams = buildWhereClause(filters)

	query = """
			SELECT DefectId
					,ComponentLocationId
					,ImageId
					,InspectionResultTimestamp
					,LineCode
					,LineName
					,StationId
					,StationName
					,StationTypeName
					,StationTypeStyleName
					,Shift
			"""
	
	if system.tag.read('[Configuration]Site/Groups_Enabled').value:
		query += """		,dbo.fn_GetGroup(LineCode, InspectionResultTimestamp) AS [Group]
			"""
	
	query +="""		,SubPlatformId
					,SubPlatformName
					,ModelName
					,SerialNumberName
					,ComponentLocationName
					,DefectDetailName
					,DefectCodeName
					,DefectTimestamp
					,Nonconformity
					,ResponsibilityName
					,AuditorBadgeNumber
					,Comment
					,StatusName
					,RepairComment
					,RepairedByBadgeNumber
					,Deleted
			FROM vw_InspectionResultDetails
			""" + whereClause + """
			ORDER BY DefectTimestamp DESC
			"""
			
	return system.db.runPrepQuery(query, queryParams, getRTYDb())


def saveDefectChanges(queryVariables):
	'''Saves defect changes to database.
		
		Args:
			queryVariables (list): description
		
		Returns:
			<return type>: description
		
		queryVariables must be in the following form:
			[responsibilityId
			,nonconformityText
			,defectCommentText
			,repairStatus
			,repairedBadgeNumber
			,repairedCommentText
			,defectCodeId
			,defectCodeName
			,defectDetailId
			,defectDetailName
			,user
			,deletedBool
			,componentLocationId
			,defectId]
	'''
	query = """
			UPDATE DEFECT 
			SET
			"""
	
	query += """ [ComponentLocationId] = cl.[ComponentLocationId]
				,[ComponentGroup] = cl.ComponentGroup
				,[ComponentClass] = cl.ComponentClass
				,[ComponentName] = cl.ComponentName
				,[ResponsibilityId] = ?
				,[Nonconformity] = ?
				,[Comment] = ?
				,[StatusId] = ?
				,[RepairedByBadgeNumber] = ?
				,[RepairComment] = ?
				,[DefectCodeId] = ?
				,[DefectCodeName] = ?
				,[DefectDetailId] = ?
				,[DefectDetailName] = ?
				,[UpdatedBy] = ?
				,[UpdatedOn] = SYSDATETIME()
				,[Deleted] = ?
			FROM Defect d
				JOIN ComponentLocation cl
					ON cl.ComponentLocationId = ?
			WHERE DefectId = ?
			"""
	
	return system.db.runPrepUpdate(query, queryVariables, getRTYDb())


def ddChangeComponentLocation(subPlatformId=None):
	'''
		ComponentLocation table that has ID/Name combination.
		Used for selecting specific components from a subPlatform.
	'''
	whereVariables = []
	query = """
			SELECT ComponentLocationId, ComponentLocationName
			FROM dbo.ComponentLocation
			"""
	
	if subPlatformId:
		query += ' WHERE SubPlatformId = ? '
		whereVariables.append(subPlatformId)
	
	query += """
			ORDER BY ComponentLocationName
			"""
	return system.db.runPrepQuery(query, whereVariables, getRTYDb()).underlyingDataset


########## 						Table Scripts 						##########
##### Calling configure cell and isCellEditable from here was too slow. #####


def onCellEdited(self, rowIndex, colIndex, colName, oldValue, newValue):
	'''<short description>
		
		<long description>
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
	if newValue != oldValue:
		sqlColumn = colName
		newComment = newValue
		oldComment = str(oldValue)
		defectId = self.data.getValueAt(rowIndex, 'DefectId')
		functionName = 'Reports/Performance/onCellEdited'
		user = getUser(None)
		
		query = """
				UPDATE dbo.Defect
				SET %s = ?,
					UpdatedBy = ?,
					UpdatedOn = GETDATE()
				WHERE DefectId = ?
				""" % sqlColumn
		queryParams = [newComment, user, defectId]
		results = system.db.runPrepUpdate(query, queryParams, getRTYDb())
		
		if results == 1:  #log if one row was changed
			logType = 'Comment Only Edit'
			log = ('DefectId = ' + str(defectId)
					+ ', SQL column = ' + sqlColumn
					+ ', Old value = ' + oldComment
					+ ', New value = ' + newComment) 
		
		else:  #more or less than one row was changed... ERROR!
			logType = 'Comment Only Edit Error'
			log = ('Rows changed = ' + str(results)
					+ ', DefectId expected = ' + str(defectId)
					+ ', sqlColumn = ' + sqlColumn
					+ ', Old value = ' + oldComment
					+ ', New value = ' + newComment)
		
		insertLog(functionName, logType, log, user)
		
		system.db.refresh(self, "data")
