'''Contains scripts specific to the Reports.Heat Map window.

	Created by: JGV	- 03-06-2020 - Split off shared functions for web services
	Updated by: WJF - 2021-05-21 - Limited buildDefectDataset to the default range.
	
	TODO:	WJF - Move dd functions to a Reports/<General, Utilities, etc.> Script file
					then fix and test all the reports screens.
'''


from shared.RTY.General import getRTYDb, datesSelectedAndInRange


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
		
		dateClause = "AND DefectTimestamp BETWEEN ? AND ? "
		filterVariables.append(startDateFormat)
		filterVariables.append(endDateFormat)
	else:
		dateClause = ""
	
	if filters['lineId']:
		lineClause = " AND L.LineId = ?"
		filterVariables.append(filters['lineId'])
	else:
		lineClause = ""
		
	if filters['shift']:
		shiftClause = " AND D.Shift = ?"
		filterVariables.append(filters['shift'])
	else:
		shiftClause = ""

	if filters['stationId']:
		stationClause = " AND S.StationId = ?"
		filterVariables.append(filters['stationId'])
	else:
		stationClause = ""
		
	if filters['platformId']:
		platformClause = " AND SP.SubPlatformId = ?"
		filterVariables.append(filters['platformId'])
	else:
		platformClause = ""
		
	if filters['componentLocationId']:
		componentLocationClause = " AND CL.ComponentLocationId = ?"
		filterVariables.append(filters['componentLocationId'])
	else:
		componentLocationClause = ""
	
	if filters['defectName'] not in ['All', 'Todo']:
		defectClause = " AND D.DefectDetailName = ?"
		filterVariables.append(filters['defectName'])
	else:
		defectClause = ""
	
	if filters['groupName'] not in ['All', 'Todo']:
		groupName = " AND dbo.fn_GetGroup(L.WhirlpoolLineCode, IR.InspectionResultTimestamp) = ? "
		filterVariables.append(filters['groupName'])
	else:
		groupName = ""
	
	whereClause = dateClause + lineClause + shiftClause + stationClause + platformClause + componentLocationClause + defectClause + groupName
	return whereClause, filterVariables


def buildDefectDataset(lineId, shift, stationId, platformId, componentLocationId, defectName, startDate, endDate, timeDimension, groupName='All'):
	'''Returns a dataset of defects from the database that fit the parameters.  Can fail on a bad date.
	
		Args:
			lineId (int):
			shift (int):
			stationId (int):
			platformId (int):
			componentLocationId (int)
			defectName (string):
			startDate (java.util.Date):
			endDate (java.util.Date):
		
		Returns:
			com.inductiveautomation...BasicDataset: Defects
	'''
	if not datesSelectedAndInRange(timeDimension, [startDate, endDate]):
		return system.dataset.toDataSet([],[])  #return a blank dataset
	
	filters = {'lineId': lineId,
				'shift': shift,
				'stationId': stationId,
				'platformId': platformId,
				'componentLocationId': componentLocationId,
				'defectName': defectName,
				'startDate': startDate,
				'endDate': endDate,
				'groupName': groupName}
	
	whereClause, filterVariables = buildWhereClause(filters)
	
	query = """
			SELECT D.DefectDetailName, D.ImageCoordinateX, D.ImageCoordinateY, D.DefectTimestamp, D.ImageId, S.StationName, SP.SubPlatformName, 
					CL.ComponentLocationName, CL.ComponentGroup, CL.ComponentLocationName, D.DefectCodeName
			FROM Defect D
				JOIN InspectionResult IR 
					ON D.InspectionResultId = IR.InspectionResultId
				JOIN Station S 
					ON IR.StationId = S.StationId
				JOIN Line L
					ON S.LineName = L.WhirlpoolLineCode
				JOIN ComponentLocation CL 
					ON D.ComponentLocationId = CL.ComponentLocationId
				LEFT JOIN Image I 
					ON CL.ImageId = I.ImageId
				JOIN SubPlatform SP
					ON CL.SubPlatformId = SP.SubPlatformId
			WHERE D.ImageCoordinateX IS NOT NULL
				AND D.ImageCoordinateY IS NOT NULL
			""" + whereClause
	return system.db.runPrepQuery(query, filterVariables, getRTYDb())


def ddComponentLocation(subPlatform=0):
	'''<short description>
		
		<long description>
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
	filterVariables = []
	
	query = """
			SELECT 0 ComponentLocationId, 'All' ComponentLocationName
			UNION
			SELECT ComponentLocationId, ComponentLocationName
			FROM dbo.ComponentLocation
			WHERE ImageId IS NOT NULL
			AND Deleted <> 1
			"""
	if subPlatform:
		query += " AND SubPlatformId = ? "
		filterVariables.append(subPlatform)
	query += """
			ORDER BY ComponentLocationName
			"""
	return system.db.runPrepQuery(query, filterVariables, getRTYDb()).underlyingDataset


def getImagePath(componentLocationId):
	'''<short description>
		
		<long description>
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
		
	query = """  
			SELECT ImageDisplayFilePath
			FROM dbo.ComponentLocation CL
				JOIN Image I
					ON CL.ImageId = I.ImageId
			WHERE ComponentLocationId = ?
			"""
	imagePath = system.db.runScalarPrepQuery(query, [componentLocationId], getRTYDb())
	#Note:  Unsure how will this work with children that have multiple parents.
	if imagePath == '': #look for parent's picture if chosen CL has is no imagePath
		query = """
				SELECT ImageDisplayFilePath
				FROM dbo.ComponentLocation CL
					JOIN Image I
						ON CL.ImageId = I.ImageId
				WHERE ComponentLocationId = (SELECT ParentLocationId
											FROM ComponentLocationMap
											WHERE ChildLocationId = ?)
				"""
		imagePath = system.db.runScalarPrepQuery(query, [componentLocationId], getRTYDb())
		
	return imagePath
		
		
def ddDefect(componentLocationId=0):
	'''<short description>
		
		<long description>
		
		Args:
			<arg name> (<arg type>): <description>
		
		Returns:
			<return type>: <description>
	'''
	filterVariables = []
	whereClause = """
					WHERE DefectDetailName != ''
						AND DefectDetailName IS NOT NULL
				"""
	
	if componentLocationId:
		whereClause += " AND D.ComponentLocationId = ? "
		filterVariables.append(componentLocationId)
	
	query = """
			SELECT 'All' DefectDetailName
			UNION
			SELECT DISTINCT DefectDetailName
			FROM dbo.Defect D
				LEFT JOIN ComponentLocation CL
					ON D.ComponentLocationId = CL.ComponentLocationId
			""" + whereClause + """
			ORDER BY DefectDetailName
			"""
	
	return system.db.runPrepQuery(query, filterVariables, getRTYDb()).underlyingDataset		