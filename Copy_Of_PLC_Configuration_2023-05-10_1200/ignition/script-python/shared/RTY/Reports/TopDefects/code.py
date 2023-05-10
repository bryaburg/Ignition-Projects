'''Contains scripts specific to the Reports.TopDefects window.
	
	Created:    JGV - 03-06-2020 - Split off shared functions for web services.
	Updated:	WJF - 2021-05-26 - Added timeDimension and related logic to 
									getTop10DefectCounts and getTopDefects.
				WJF - 2021-08-02 - Added ddGroup().
				WJF - 2021-08-04 - Added group and cleaned buildWhereClause().
								 - Added group to getTop10DefectCounts().
								 - Added group to getTopDefects().
	
	TODO:	WJF - Move dd functions to a Reports/<General, Utilities, etc.> Script file
					then fix and test all the reports screens.
'''


from shared.RTY.General import getRTYDb, getLodestarDb, datesSelectedAndInRange


def buildWhereClause(filters={}):
	'''Builds a SQL WHERE clause and list of variables.
										
	<long description>
	
	Args:
		filters (dict): Dictionary of filter values keyed to their types.
	
	Returns:
		text: whereClause - SQL text in the form of WHERE... AND... AND...
		list: filterVariables - list of filter values in order of appearance in the whereClause.
	'''
	filterVariables = []
	
	if filters['startDate'] and filters['endDate']:
		formatForSQL = 'yyyy-MM-dd HH:mm:ss'
		startDateFormat = system.date.format(filters['startDate'], formatForSQL)
		endDateFormat = system.date.format(filters['endDate'], formatForSQL)
		
		dateClause = " AND DefectTimestamp BETWEEN ? AND ? "
		filterVariables.append(startDateFormat)
		filterVariables.append(endDateFormat)
	else:
		dateClause = ""
	
	if filters['lineId']:
		lineClause = " AND L.LineId = ? "
		filterVariables.append(filters['lineId'])
	else:
		lineClause = ""
				
	if filters['stationId']:
		stationClause = " AND S.StationId = ? "
		filterVariables.append(filters['stationId'])
	else:
		stationClause = ""

	if filters['shift']:
		shiftClause = " AND D.Shift = ? "
		filterVariables.append(filters['shift'])
	else:
		shiftClause = ""
		
	if filters['platformId']:
		platformClause = " AND SP.SubPlatformId = ? "
		filterVariables.append(filters['platformId'])
	else:
		platformClause = ""

	if filters['componentLocationName'] not in ['All', 'Todo']:
		locationClause = " AND CL.ComponentLocationName = ? "
		filterVariables.append(filters['componentLocationName'])
	else:
		locationClause = ""
	
	if filters['groupName'] not in ['All', 'Todo']:
		groupClause = " AND dbo.fn_GetGroup(L.WhirlpoolLineCode, IR.InspectionResultTimestamp) = ? "
		filterVariables.append(filters['groupName'])
	else:
		groupClause = ""
		
	whereClause = ("WHERE SN.Test = 0 " 
				+ dateClause 
				+ lineClause 
				+ stationClause 
				+ shiftClause 
				+ platformClause 
				+ locationClause 
				+ groupClause)
				
	return whereClause, filterVariables
	
	
def getTop10DefectCounts(lineId=None, stationId=None, shift=None, platformId=None, componentLocationName=None,
							startDate=None, endDate=None, timeDimension=-1, groupName='All'):
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	if not datesSelectedAndInRange(timeDimension, [startDate, endDate]):
		return system.dataset.toDataSet(['Defect', 'DefectCount'],[])  #return a blank dataset
	
	filters = {'lineId': lineId,
				'shift': shift,
				'stationId': stationId,
				'platformId': platformId,
				'componentLocationName': componentLocationName,
				'startDate': startDate,
				'endDate': endDate,
				'groupName': groupName}
				
	whereClause, filterVariables = buildWhereClause(filters)
	
	query = """
			SELECT TOP 10 CONCAT(D.DefectDetailName, ' - ', D.DefectCodeName, ' - ', CL.ComponentLocationName) AS 'Defect'
						,COUNT(CONCAT(D.DefectDetailId, ' - ', D.DefectCodeId)) AS DefectCount
			FROM Defect D
				JOIN ComponentLocation CL 
					ON D.ComponentLocationId = CL.ComponentLocationId
				JOIN InspectionResult IR 
					ON D.InspectionResultId = IR.InspectionResultId
				JOIN SubPlatform SP 
					ON CL.SubPlatformId = SP.SubPlatformId
				JOIN Station S 
					ON IR.StationId = S.StationId
				JOIN Line L 
					ON S.LineName = L.WhirlpoolLineCode
				LEFT JOIN SerialNumber SN 
					ON IR.SerialNumberId = SN.SerialNumberId
				LEFT JOIN Model M 
					ON SN.ModelId = M.ModelId
			""" + whereClause + """
			GROUP BY CONCAT(D.DefectDetailName, ' - ', D.DefectCodeName, ' - ', CL.ComponentLocationName)
			ORDER BY DefectCount DESC
			"""

	return system.db.runPrepQuery(query, filterVariables, getRTYDb())
		
		
def getTopDefects(topCount=10, lineId=None, stationId=None, shift=None, platformId=None, componentLocationName=None,
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
	
	filters = {'lineId': lineId,
				'shift': shift,
				'stationId': stationId,
				'platformId': platformId,
				'componentLocationName': componentLocationName,
				'startDate': startDate,
				'endDate': endDate,
				'groupName': groupName}
	
	whereClause, filterVariables = buildWhereClause(filters)
	
	filterVariables.insert(0, topCount)
	
	query = """ SELECT TOP (?) 
					D.DefectTimestamp
					,D.Shift
					,L.LineName
					,SP.SubPlatformName
					,M.ModelName
					,SN.SerialNumberName
					,CL.ComponentLocationName
					,D.ComponentGroup
					,D.ComponentName
					,D.DefectDetailName
					,D.DefectCodeName
					,D.Nonconformity
			"""
			
	if system.tag.read('[Configuration]Site/Groups_Enabled').value:
		query += "			,dbo.fn_GetGroup(L.WhirlpoolLineCode, IR.InspectionResultTimestamp) AS [Group]"
					
	query += """
			FROM Defect D
				JOIN ComponentLocation CL 
					ON D.ComponentLocationId = CL.ComponentLocationId
				JOIN InspectionResult IR
					ON D.InspectionResultId = IR.InspectionResultId
				JOIN SubPlatform SP
					ON CL.SubPlatformId = SP.SubPlatformId
				JOIN Station S
					ON IR.StationId = S.StationId
				JOIN Line L
					ON S.LineName = L.WhirlpoolLineCode
				LEFT JOIN SerialNumber SN 
					ON IR.SerialNumberId = SN.SerialNumberId
				LEFT JOIN Model M 
					ON SN.ModelId = M.ModelId
			""" + whereClause + """
			ORDER BY D.DefectId DESC
			"""			
	
	return system.db.runPrepQuery(query, filterVariables, getRTYDb())


def ddComponentLocation(subPlatform=0, all=1):
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	whereVariables = []
	
	allClause = """
				SELECT 'All' ComponentLocationName
				UNION
				"""
	query = """
			SELECT ComponentLocationName
			FROM dbo.ComponentLocation
			"""
	if all:
		query = allClause + query
		
	if subPlatform:
		query += 'WHERE SubPlatformId = ? '
		whereVariables.append(subPlatform)

	query += """
			ORDER BY ComponentLocationName
			"""
	return system.db.runPrepQuery(query, whereVariables, getRTYDb()).underlyingDataset


def ddLineLoad(areaName='All'):
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	whereVariables = []
	
	query = """
			SELECT 0 LineId, 'All' LineName
			UNION
			SELECT L.LineId, L.LineName
			FROM dbo.Line L
				LEFT JOIN dbo.Station S
					ON L.WhirlpoolLineCode = S.LineName
			"""
	if areaName not in ['All', 'Todo']:
		query += "WHERE S.AreaName = ?"
		whereVariables.append(areaName)
				
	return system.db.runPrepQuery(query, whereVariables, getRTYDb()).underlyingDataset

		
def ddPlatform(lineId=0):
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	whereVariables = []
	
	query = """
			SELECT 0 SubPlatformId, 'All' SubPlatformName
			UNION
			SELECT SubPlatformId, SubPlatformName
			FROM [dbo].[LINE] L
				JOIN [dbo].[LinePlatformMap] LP
					ON L.LineId = LP.LineId
				JOIN [dbo].[SubPlatform] SP
					ON LP.PlatformId = SP.PlatformId
			"""
	if lineId:
		query += ' WHERE L.LineId = ? '
		whereVariables.append(lineId)
		
	return system.db.runPrepQuery(query, whereVariables, getRTYDb()).underlyingDataset
	
	
def ddShift(lineId=0):
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	whereVariables = []
	
	query = """
			SELECT 0 ShiftId, 'All' ShiftName
			UNION
			SELECT DISTINCT DEFINITION_NUMBER AS ShiftId, CAST(DEFINITION_NUMBER AS varchar) AS ShiftName
			FROM dbo.ShiftHistory
			"""
	if lineId:
		query += ' WHERE EQUIPMENT_ID = ? '
		whereVariables.append(lineId)
		
	return system.db.runPrepQuery(query, whereVariables, getRTYDb()).underlyingDataset
	

def ddStationType():
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	whereVariables = []
	
	query = """
			SELECT 0 StationTypeId, 'All' StationTypeName
			UNION
			SELECT StationTypeId, StationTypeName
			FROM dbo.StationType ST
			"""
	return system.db.runPrepQuery(query, whereVariables, getRTYDb()).underlyingDataset


def ddStation(lineId=0, stationTypeId=0):
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	whereVariables = []
	
	query = """
			SELECT 0 StationId, 'All' StationName
			UNION
			SELECT StationId, StationName
			FROM dbo.Station S
				JOIN dbo.Line L
					ON S.LineName = L.WhirlpoolLineCode
			WHERE 1=1
			"""
	if lineId:
		query += ' AND L.LineId = ? '
		whereVariables.append(lineId)
		
	if stationTypeId:
		query += ' AND S.stationType = ? '
		whereVariables.append(lineId)
	
	return system.db.runPrepQuery(query, whereVariables, getRTYDb()).underlyingDataset


def ddDefect(componentLocationName='All', all=1):
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	whereVariables = []
	whereClause = ''
	if componentLocationName not in ['All', 'Todo']:
		whereClause += " AND ComponentLocationName = ? "
		whereVariables.append(componentLocationName)
	
	allClause = """
			SELECT 'All' DefectDetailName
			UNION
		"""
	query = """
			SELECT DISTINCT DefectDetailName
			FROM dbo.Defect D
				LEFT JOIN ComponentLocation CL
					ON D.ComponentLocationId = CL.ComponentLocationId
			WHERE DefectDetailName != ''
				AND DefectDetailName IS NOT NULL
			""" + whereClause + """
			ORDER BY DefectDetailName
			"""
	if all:
		query = allClause + query		
	
	return system.db.runPrepQuery(query, whereVariables, getRTYDb()).underlyingDataset


def ddArea():
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	query = """
			SELECT 'All' AreaName
			UNION
			SELECT DISTINCT AreaName
			FROM dbo.Station
			WHERE Deleted = 0
			"""
	return system.db.runQuery(query, getRTYDb()).underlyingDataset
	
	
def ddNonconformity():
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	query = """
			SELECT ROW_NUMBER() OVER(ORDER BY REPAIR_CODE) AS [btnId], CONCAT(REPAIR_CODE, ' - ', REPAIR_CODE_NAME) AS [btnName]
			FROM [dbo].[RepairCode]
			WHERE REPAIR_CODE_TYPE = 'NonConformity'
			"""
	return system.db.runQuery(query, getLodestarDb()).underlyingDataset
		
		
def ddResponsibility(stationID=0):
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	query = """
			SELECT 0 as Value, 'All' as Label
			UNION
			SELECT r.ResponsibilityId as Value, r.ResponsibilityName as Label
			FROM [dbo].[Station] s
				JOIN [dbo].[StationResponsibility] sr
					ON s.StationId = sr.StationId
					AND s.StationId = ?
				JOIN [dbo].[Responsibility] r
					ON sr.ResponsibilityId = r.ResponsibilityId
			"""
	return system.db.runPrepQuery(query, [stationID], getRTYDb()).underlyingDataset


def ddGroup():
	'''Returns dataset of Groups as Value/Label pairs.
	
	Pairs are ordered alphabetically and a generic number is applied.
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	query = """
			SELECT 0 as Value, 'All' as Label
			UNION
			SELECT ROW_NUMBER() OVER(ORDER BY G.GROUP_R1) AS [Value]
				,G.GROUP_R1 AS [Label]
			FROM (
					SELECT DISTINCT [GROUP_R1]
					FROM [Lodestar_RTY].[dbo].[ShiftHistory]
					WHERE [GROUP_R1] NOT IN ('None', '', '0')
						AND [GROUP_R1] IS NOT NULL
				) G
			"""
	return system.db.runPrepQuery(query, [], getRTYDb()).underlyingDataset
