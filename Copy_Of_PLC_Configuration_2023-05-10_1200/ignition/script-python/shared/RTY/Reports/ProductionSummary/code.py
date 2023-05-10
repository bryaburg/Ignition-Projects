'''Contains scripts specific to the Reports.ProductionSummary window.
	
	Created:	JGV - 03-06-2020 - Split off shared functions for web services
	Updated: 	WJF - 2020-04-20 - Replaced getLodestarDb with getLodestarDbName
					- 2021-05-25 - Added timeDimension and related logic to updateTable.
					- 2021-08-05 - Added group to updateTable() and buildFilterWhereClause().
								 - Created buildSummaryQueryWithShiftAndGroup().
'''


from shared.RTY.General import getRTYDb, getLodestarDbName, datesSelectedAndInRange
	

def updateTable(shift, areaName=None, lineName=None, shiftNumber=None, startDate=None, endDate=None, timeDimension=-1, groupName='All'):
	'''<short description>
		
		Called in expression bindings to the data tables of both
		LineOnly and LineAndShift.
		This is why Shift specific where clause is separated from whereClause.
		
		Args:
			<arg name> (<arg type>): description
		
		Returns:
			<return type>: description
	'''
	if not datesSelectedAndInRange(timeDimension, [startDate, endDate]):
		return system.dataset.toDataSet([],[])  #return a blank dataset
		
	if areaName in ['All', 'Todo']:
		areaName = None
	if lineName in ['All', 'Todo']:
		lineName = None
	if groupName in ['All', 'Todo']:
		groupName = None

	filters = {'areaName': areaName,
				'lineName': lineName,
				'startDate': startDate,
				'endDate': endDate,
				'groupName': groupName}
	whereClause, filterVar = buildFilterWhereClause(filters)
	
	filterAndShiftVar = filterVar
	totalFilterVar = filterVar + filterVar
	
	shiftClause = ""
	if shiftNumber:
		shiftClause = " WHERE [Shift] = ?"
		filterAndShiftVar.append(shiftNumber)
	
	totalFilterAndShiftVar = filterAndShiftVar + filterAndShiftVar
	
	if shift:
		if system.tag.read('[Configuration]Site/Groups_Enabled').value:
			query = buildSummaryQueryWithShiftAndGroup(whereClause, shiftClause)
		else:
			query = buildSummaryQueryWithShiftQuery(whereClause, shiftClause)
		return system.db.runPrepQuery(query, totalFilterAndShiftVar, getRTYDb())
	
	else:
		query = buildSummaryQueryWithoutShiftQuery(whereClause)
		return system.db.runPrepQuery(query, totalFilterVar, getRTYDb())
	


def buildFilterWhereClause(filters):
	'''
		Takes a dictionary of all the filters.
		Returns a SQL Where Clause built from these filters.
	'''
	filterVariables = []
	
	if filters['startDate'] and filters['endDate']:
		formatForSQL = 'yyyy-MM-dd HH:mm:ss'
		startDateFormat = system.date.format(filters['startDate'], formatForSQL)
		endDateFormat = system.date.format(filters['endDate'], formatForSQL)
		
		dateClause = " AND IR.InspectionResultTimestamp BETWEEN ? AND ?"
		filterVariables.append(startDateFormat)
		filterVariables.append(endDateFormat)
	else:
		dateClause = ""

	if filters['areaName']:
		areaClause = " AND S.AreaName = ?"
		filterVariables.append(filters['areaName'])
	else:
		areaClause = ""
	
	if filters['lineName']:
		lineClause = " AND L.LineName = ?"
		filterVariables.append(filters['lineName'])
	else:
		lineClause = ""
	
	if filters['groupName']:
		groupClause = "  AND dbo.fn_GetGroup(L.WhirlpoolLineCode, IR.InspectionResultTimestamp) = ? "
		filterVariables.append(filters['groupName'])
	else:
		groupClause = ""
		
	whereClause =  """
					WHERE IR.InspectionResultId IS NOT NULL 
						AND SN.Test = 0 """ + dateClause + areaClause + lineClause + groupClause
	return whereClause, filterVariables


def buildSummaryQueryWithShiftQuery(filterWhereClause, shiftWhereClause):
	'''
		docstring
	'''
	return """
			DECLARE	@calCategory table (calName nvarchar(50)
										,calWeight real)
			
			INSERT INTO @calCategory
				SELECT CONCAT(RC.REPAIR_CODE, ' - ', RC.REPAIR_CODE_NAME) AS calName, RV.REPAIR_CODE AS calWeight
				FROM """ + getLodestarDbName() + """.dbo.RepairCode RC
					JOIN """ + getLodestarDbName() + """.dbo.RepairCode RV
						ON RC.REPAIR_CODE = RV.REPAIR_CODE_NAME
				WHERE RC.REPAIR_CODE_TYPE = 'NonConformity'
			
			DECLARE	@calCalculation table (AreaName nvarchar(50)
											,LineName nvarchar(50)
											,[Shift] nvarchar(50)
											,CALTested bigint
											,CAL real)
			
			INSERT INTO @calCalculation
				SELECT AreaName
					,LineName
					,[Shift]
					,CALTested = COUNT(SerialNumberId)
					--,calWeight = COUNT(calWeight) 
					,CAL = CAST(COUNT(calWeight) AS REAL) / COUNT(SerialNumberId) * 100
			
				FROM(
					SELECT S.AreaName
							,S.LineName
							,[Shift] = ISNULL(D.[Shift], dbo.fn_GetShiftNumber(L.WhirlpoolLineCode, IR.InspectionResultTimestamp))
							,SN.SerialNumberId
							,C.calWeight
					FROM Station S
						JOIN InspectionResult IR
							ON S.StationId = IR.StationId
							AND StationName LIKE '%cal%'
						JOIN SerialNumber SN
							ON IR.SerialNumberId = SN.SerialNumberId
						LEFT JOIN Defect D
							ON IR.InspectionResultId = D.InspectionResultId
						LEFT JOIN @calCategory C
							ON D.Nonconformity = C.calName
							AND C.calWeight = 1.0
						LEFT JOIN Line L
							ON S.LineName = L.WhirlpoolLineCode
					""" + filterWhereClause + """
					
					)DataTable
				""" + shiftWhereClause + """
				GROUP BY AreaName, LineName, [Shift]
				ORDER BY AreaName, LineName, [Shift]
			
			SELECT [data].AreaName
				,[data].LineName
				,[data].[SHIFT]
				,isTotal
				,isDefect
				,FPY = CAST((isTotal - isDefect) AS real) / isTotal * 100
				,CALTested = ISNULL(CALTested, 0)
				,CAL = ISNULL(CAL, 0)
			FROM (	
					SELECT	AreaName
							,LineName
							,[Shift]
							,isTotal = COUNT(DISTINCT SerialNumberId)
							,isDefect = COUNT(DISTINCT CASE WHEN DefectId IS NOT NULL THEN SerialNumberId ELSE NULL END)
					FROM(	
							SELECT	S.AreaName
									,S.LineName
									,[Shift] = ISNULL(D.[Shift], dbo.fn_GetShiftNumber(L.WhirlpoolLineCode, IR.InspectionResultTimestamp))
									,S.StationName
									,SN.SerialNumberId
									,D.DefectId
							FROM dbo.SerialNumber SN
								LEFT JOIN InspectionResult IR
									ON SN.SerialNumberId = IR.SerialNumberId
								LEFT JOIN Defect D
									ON IR.InspectionResultId = D.InspectionResultId
										AND D.Deleted = 0
								LEFT JOIN Station S
									ON IR.StationId = S.StationId
								LEFT JOIN Line L
									ON S.LineName = L.WhirlpoolLineCode
							""" + filterWhereClause + """
			
						) RawDataTable
					""" + shiftWhereClause + """
					GROUP BY AreaName, LineName, [Shift]
			
				) [data]
				LEFT JOIN @calCalculation C
					ON [data].AreaName = C.AreaName
						AND [data].LineName = C.LineName
						AND [data].[Shift] = C.[Shift]
			ORDER BY [data].AreaName, [data].LineName, [data].[SHIFT]
			"""


def buildSummaryQueryWithoutShiftQuery(filterWhereClause):
	'''
		Docstring
	''' 
	return """
			DECLARE	@calCategory table (calName nvarchar(50)
										,calWeight real)
			
			INSERT INTO @calCategory
				SELECT CONCAT(RC.REPAIR_CODE, ' - ', RC.REPAIR_CODE_NAME) AS calName, RV.REPAIR_CODE AS calWeight
				FROM """ + getLodestarDbName() + """.dbo.RepairCode RC
					JOIN """ + getLodestarDbName() + """.dbo.RepairCode RV
						ON RC.REPAIR_CODE = RV.REPAIR_CODE_NAME
				WHERE RC.REPAIR_CODE_TYPE = 'NonConformity'
			
			DECLARE	@calCalculation table (AreaName nvarchar(50)
											,LineName nvarchar(50)
											,CALTested bigint
											,CAL real)
			
			INSERT INTO @calCalculation
				SELECT AreaName
					,LineName
					,CALTested = COUNT(SerialNumberId)
					--,calWeight = COUNT(calWeight) 
					,CAL = CAST(COUNT(calWeight) AS REAL) / COUNT(SerialNumberId) * 100
			
				FROM(
					SELECT S.AreaName
							,S.LineName
							,SN.SerialNumberId
							,C.calWeight
					FROM Station S
						JOIN InspectionResult IR
							ON S.StationId = IR.StationId
							AND StationName LIKE '%cal%'
						JOIN SerialNumber SN
							ON IR.SerialNumberId = SN.SerialNumberId
						LEFT JOIN Defect D
							ON IR.InspectionResultId = D.InspectionResultId
						LEFT JOIN @calCategory C
							ON D.Nonconformity = C.calName
							AND C.calWeight = 1.0
						LEFT JOIN Line L
							ON S.LineName = L.WhirlpoolLineCode
					""" + filterWhereClause + """
					)DataTable
				GROUP BY AreaName, LineName
				ORDER BY AreaName, LineName
			
			SELECT FilteredTable.AreaName
				,FilteredTable.LineName
				,isTotal
				,isDefect
				,FPY = CAST((isTotal - isDefect) AS real) / isTotal * 100
				,CALTested = ISNULL(CALTested, 0)
				,CAL = ISNULL(CAL, 0)
			FROM (	
					SELECT	AreaName
							,LineName
							,isTotal = COUNT(DISTINCT SerialNumberId)
							,isDefect = COUNT(DISTINCT CASE WHEN DefectId IS NOT NULL THEN SerialNumberId ELSE NULL END)
					FROM(	
							SELECT	S.AreaName
									,S.LineName
									,SN.SerialNumberId
									,D.DefectId
							FROM dbo.SerialNumber SN
								LEFT JOIN InspectionResult IR
									ON SN.SerialNumberId = IR.SerialNumberId
								LEFT JOIN Defect D
									ON IR.InspectionResultId = D.InspectionResultId
										AND D.Deleted = 0
								LEFT JOIN Station S
									ON IR.StationId = S.StationId
								LEFT JOIN Line L
									ON S.LineName = L.WhirlpoolLineCode
							""" + filterWhereClause + """
		
						) RawData
					GROUP BY AreaName, LineName
		
				) FilteredTable
				LEFT JOIN @calCalculation C
					ON FilteredTable.AreaName = C.AreaName
						AND FilteredTable.LineName = C.LineName
		
			ORDER BY FilteredTable.AreaName, FilteredTable.LineName
			"""


def buildSummaryQueryWithShiftAndGroup(filterWhereClause, shiftWhereClause):
	"""Returns text for the summary query with shift and group.
	
		Args:
			filterWhereClause (text): SQL text contains the filters.
			shiftWhereClause (text): SQL text contains the shift specific filter.
		
		Returns:
			text:  SQL Query	
	
		WJF TODO:  Rewrite me!  I'm Terrible!
	"""
	return """
			DECLARE	@calCategory table (calName nvarchar(50)
					,calWeight real)
	
			INSERT INTO @calCategory
				SELECT CONCAT(RC.REPAIR_CODE, ' - ', RC.REPAIR_CODE_NAME) AS calName, RV.REPAIR_CODE AS calWeight
				FROM """ + getLodestarDbName() + """.dbo.RepairCode RC
					JOIN """ + getLodestarDbName() + """.dbo.RepairCode RV
						ON RC.REPAIR_CODE = RV.REPAIR_CODE_NAME
				WHERE RC.REPAIR_CODE_TYPE = 'NonConformity'
			
			DECLARE	@calCalculation table (AreaName nvarchar(50)
											,LineName nvarchar(50)
											,[Shift] nvarchar(50)
											,[Group] nvarchar(50)
											,CALTested bigint
											,CAL real)
			
			INSERT INTO @calCalculation
				SELECT AreaName
					,LineName
					,[Shift]
					,[Group]
					,CALTested = COUNT(SerialNumberId)
					--,calWeight = COUNT(calWeight) 
					,CAL = CAST(COUNT(calWeight) AS REAL) / COUNT(SerialNumberId) * 100
			
				FROM(
					SELECT S.AreaName
							,S.LineName
							,[Shift] = ISNULL(D.[Shift], dbo.fn_GetShiftNumber(L.WhirlpoolLineCode, IR.InspectionResultTimestamp))
							,[Group] = dbo.fn_GetGroup(L.WhirlpoolLineCode, IR.InspectionResultTimestamp)
							,SN.SerialNumberId
							,C.calWeight
					FROM Station S
						JOIN InspectionResult IR
							ON S.StationId = IR.StationId
							AND StationName LIKE '%cal%'
						JOIN SerialNumber SN
							ON IR.SerialNumberId = SN.SerialNumberId
						LEFT JOIN Defect D
							ON IR.InspectionResultId = D.InspectionResultId
						LEFT JOIN @calCategory C
							ON D.Nonconformity = C.calName
							AND C.calWeight = 1.0
						LEFT JOIN Line L
							ON S.LineName = L.WhirlpoolLineCode
					""" + filterWhereClause + """
					
					)DataTable
				""" + shiftWhereClause + """
				GROUP BY AreaName
						,LineName
						,[Shift]
						,[Group]
				ORDER BY AreaName
						,LineName
						,[Shift]
						,[Group]
			
			SELECT [data].AreaName
				,[data].LineName
				,[data].[Shift]
				,[data].[Group]
				,isTotal
				,isDefect
				,FPY = CAST((isTotal - isDefect) AS real) / isTotal * 100
				,CALTested = ISNULL(CALTested, 0)
				,CAL = ISNULL(CAL, 0)
			FROM (	
					SELECT	AreaName
							,LineName
							,[Shift]
							,[Group]
							,isTotal = COUNT(DISTINCT SerialNumberId)
							,isDefect = COUNT(DISTINCT CASE WHEN DefectId IS NOT NULL THEN SerialNumberId ELSE NULL END)
					FROM(	
							SELECT	S.AreaName
									,S.LineName
									,[Shift] = ISNULL(D.[Shift], dbo.fn_GetShiftNumber(L.WhirlpoolLineCode, IR.InspectionResultTimestamp))
									,[Group] = dbo.fn_GetGroup(L.WhirlpoolLineCode, IR.InspectionResultTimestamp)
									,S.StationName
									,SN.SerialNumberId
									,D.DefectId
							FROM dbo.SerialNumber SN
								LEFT JOIN InspectionResult IR
									ON SN.SerialNumberId = IR.SerialNumberId
								LEFT JOIN Defect D
									ON IR.InspectionResultId = D.InspectionResultId
										AND D.Deleted = 0
								LEFT JOIN Station S
									ON IR.StationId = S.StationId
								LEFT JOIN Line L
									ON S.LineName = L.WhirlpoolLineCode
							""" + filterWhereClause + """
			
						) RawDataTable
					""" + shiftWhereClause + """
					GROUP BY AreaName
							,LineName
							,[Shift]
							,[Group]
			
				) [data]
				LEFT JOIN @calCalculation C
					ON [data].AreaName = C.AreaName
						AND [data].LineName = C.LineName
						AND [data].[Shift] = C.[Shift]
						AND [data].[GROUP] = C.[GROUP]
			ORDER BY [data].AreaName
					,[data].LineName
					,[data].[SHIFT]
					,[data].[Group]
			"""
