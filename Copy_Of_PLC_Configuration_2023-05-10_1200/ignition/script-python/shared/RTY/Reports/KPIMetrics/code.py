'''
	Created by/on:    JGV		03-06-2020		Split off shared functions for web services
	
	Contains scripts specific to the Reports.KPI Metrics window.
	
	Updated by:		WJF - 2020-04-20 - Replaced getLodestarDb with getLodestarDbName
	Updated by:			- 2021-03-08 - Changed RTY = FPY * FPY * ... * FPY to
									   Station RTY = (UniqueSerialNumbers - AllDefectsOnThoseSerials)/ UniqueSerialNumbers
									   Total RTY = Station RTY * Station RTY * ... * Station RTY
						- 2021-03-15 - Change listed above was reversed.
						- 2021-05-25 - Added date limiter functionality to kpiDataCollector.
	
	TODO:  WJF - Reduce parameter load in the kpiDataCollector.
'''


from shared.RTY.General import getRTYDb, getLodestarDbName, datesSelectedAndInRange


def kpiDataCollector(areaName=None, lineName=None, shiftId=None, stationId=None, platformId=None,
						startDate=None, endDate=None, xAxis="Area", timeLevel=None, timeDimension=0, groupName='All'):
	'''
		Takes user selections from kpi screen.
		Formats dates to SQL format.
		Grabs CAL data from R1.
		Builds a groupBy table based on the user-selected xAxis.
		Builds a whereClause based on users-selected filters.
		Combines all of this in a Serial Number table query.
		Returns dataset of [x, fpy, cal, rty].
	'''
	if not datesSelectedAndInRange(timeDimension, [startDate, endDate]):
		return system.dataset.toDataSet(['x', 'FPY', 'CAL', 'RTY'],[])  #return a blank dataset
	
	if xAxis in ['<Select One>', '<Seleccionar uno>']:
		xAxis = 'Area'
	if areaName in ['All', 'Todo']:
		areaName = None
	if lineName in ['All', 'Todo']:
		lineName = None
	if groupName in ['All', 'Todo']:
		groupName = None
	
	formatForSQL = 'yyyy-MM-dd HH:mm:ss'
	startDateFormat = system.date.format(startDate, formatForSQL)
	endDateFormat = system.date.format(endDate, formatForSQL)
	
	filters = {'areaName': areaName,
				'lineName': lineName,
				'shiftId': shiftId,
				'stationId': stationId,
				'platformId': platformId,
				'startDate': startDateFormat,
				'endDate': endDateFormat,
				'timeLevel': timeLevel,
				'groupName': groupName}
				
	calTable = buildCALDefinitionTable(getLodestarDbName())
	groupByTable, groupByJoinClause = buildGroupBySQL(xAxis, filters)
	whereClause, filterVariables = buildFilterWhereClause(filters)
	rawDataTable = buildRawDataTable(whereClause)
	kpiQuery = buildKPICalculator(calTable, groupByTable, groupByJoinClause, rawDataTable, xAxis, filters)
	
	return system.db.runPrepQuery(kpiQuery, filterVariables, getRTYDb())
	
	
def buildCALDefinitionTable(lodestarDbName):
	'''
		Returns SQL-formatted string that will fetch CAL Names and Weights from the R1 database.
	'''
	return """
			DECLARE	@cal table(
						calName nvarchar(50),
						calWeight real
						)
	
			INSERT INTO @cal
				SELECT CONCAT(RC.REPAIR_CODE, ' - ', RC.REPAIR_CODE_NAME) AS calName, RV.REPAIR_CODE AS calWeight
				FROM """ + lodestarDbName + """.dbo.RepairCode RC
					JOIN """ + lodestarDbName + """.dbo.RepairCode RV
						ON RC.REPAIR_CODE = RV.REPAIR_CODE_NAME
				WHERE RC.REPAIR_CODE_TYPE = 'NonConformity'
			"""
	
	
def buildFilterWhereClause(filters):
	'''
		Takes a dictionary of all the filters.
		Returns a SQL Where Clause built from these filters.
	'''
	filterVariables = []
	
	if filters['startDate'] and filters['endDate']:
		dateClause = " AND IR.InspectionResultTimestamp BETWEEN ? AND ?"
		filterVariables.append(filters['startDate'])
		filterVariables.append(filters['endDate'])
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
		
	if filters['shiftId']:
		shiftClause = " AND ISNULL([Shift], dbo.fn_GetShiftNumber(WhirlpoolLineCode, InspectionResultTimestamp)) = ?"
		filterVariables.append(filters['shiftId'])
	else:
		shiftClause = ""

	if filters['stationId']:
		stationClause = " AND S.StationId = ?"
		filterVariables.append(filters['stationId'])
	else:
		stationClause = ""
		
	if filters['platformId']:
		platformClause = " AND IR.PlatformId = ?"
		filterVariables.append(filters['platformId'])
	else:
		platformClause = ""
	
	if filters['groupName']:
		groupClause = " AND dbo.fn_GetGroup(L.WhirlpoolLineCode, IR.InspectionResultTimestamp) = ? "
		filterVariables.append(filters['groupName'])
	else:
		groupClause = ""
	
	whereClause =  """
					WHERE IR.InspectionResultId IS NOT NULL
						AND SN.Test = 0 """ + dateClause + areaClause + lineClause + shiftClause + stationClause + platformClause + groupClause
	return whereClause, filterVariables


def buildGroupBySQL(xAxis, filters):
	'''
		Takes xAxis.
		Returns SQL String that will return a table that contains ids and unique x-coordinate values 
		--And startDates when that is implemented and Time selected by the user.
		Returns SQL String that will tell the SerialNumber table how to GroupBy the provided x-coordinates.
	'''
	query = """
			DECLARE @groupBy table(id int, xCoordinate nvarchar(50))
			"""
			
	groupByJoin = ''
	
	if xAxis in ['Area', u'Área']:
		query += """
				INSERT INTO @groupBy
				SELECT ROW_NUMBER() OVER(ORDER BY AreaName) AS ID, AreaName
				FROM (SELECT DISTINCT AreaName
					FROM Station) R
				"""
		groupByJoin = """
					ON AreaName = G.xCoordinate
					"""

	elif xAxis in ['Line', u'Línea']:
		query += """
				INSERT INTO @groupBy
				SELECT ROW_NUMBER() OVER(ORDER BY LineName) AS ID, LineName
				FROM (SELECT DISTINCT LineName
						FROM Station) R
				"""
		groupByJoin = """
					ON WhirlpoolLineCode = G.xCoordinate
					"""

	elif xAxis in ['Shift', 'Turno']:
		query += """
				INSERT INTO @groupBy
				VALUES
				(1, '1'), (2, '2'), (3, '3')
				"""
		groupByJoin = """
					ON ISNULL([Shift], dbo.fn_GetShiftNumber(WhirlpoolLineCode, InspectionResultTimestamp)) = G.xCoordinate
					"""

	elif xAxis in ['Time', 'Tiempo']:
		query += """
				INSERT INTO @groupBy
					SELECT TOP (DATEDIFF(""" + filters['timeLevel'] +  ", '"  + filters['startDate'] + "', '" + filters['endDate'] + """') + 1)
						ROW_NUMBER() OVER(ORDER BY a.object_id) AS id
						,CONVERT(nvarchar, DATEADD(""" + filters['timeLevel'] + ", ROW_NUMBER() OVER(ORDER BY a.object_id) - 1, '" + filters['startDate'] + """'), 13) AS xCoordinate
					FROM sys.all_objects a
						CROSS JOIN sys.all_objects b;
				"""
		groupByJoin = """
					ON DATEADD(""" + filters['timeLevel'] + ", DATEDIFF(" + filters['timeLevel'] + """, 0, InspectionResultTimestamp), 0) = G.xCoordinate
					"""
				
	return query, groupByJoin
	

def buildRawDataTable(filterWhereClause, includeCAL=True):
	'''
		Takes Filtered Where Clause and applies it to a raw SQL table.
		The calculations and groupings are run on this rawData table.
	'''
	calDeclare = ''
	calInsert = ''
	calFrom = ''
	if includeCAL:
		calDeclare = ',calName nvarchar(50)'
		calInsert = ',C.calName'
		calFrom = """
					LEFT JOIN @cal C
						ON D.Nonconformity = C.calName
							AND C.calWeight = 1	
					"""
	return """
			DECLARE @rawData table(SerialNumberId bigint
							,InspectionResultTimestamp datetime
							,StationId bigint
							,StationName nvarchar(50)
							,AreaName nvarchar(50)
							,DefectId bigint
							,[Shift] int
							,LineId bigint
							,LineName nvarchar(256)
							,WhirlpoolLineCode nvarchar(10)
							""" + calDeclare + """
							)
							
			INSERT INTO @rawData
				SELECT	SN.SerialNumberId
						,IR.InspectionResultTimestamp
						,S.StationId
						,S.StationName
						,S.AreaName
						,D.DefectId
						,D.Shift
						,L.LineId
						,L.LineName
						,L.WhirlpoolLineCode
						""" + calInsert + """
									
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
					""" + calFrom + """
			""" + filterWhereClause
	

def calculateFPY(groupByJoinClause):
	'''
		Given a data and groupBy table (and associated GroupByJoinClause)
		Will calculate FPY.
	'''
	return """
			SELECT xCoordinate
				,FPY = CAST((totalSN - allDefects) AS real) / totalSN * 100
			FROM (
					SELECT	G.id
							,G.xCoordinate
							,totalSN = COUNT(DISTINCT SerialNumberId)
							,allDefects = COUNT(DISTINCT CASE WHEN DefectId IS NOT NULL THEN SerialNumberId ELSE NULL END)
					FROM (
						@rawData					--required table
						LEFT JOIN @GroupBy G		--required table
							""" + groupByJoinClause + """
						)
					GROUP BY id, xCoordinate
				) groupTable
			"""


def calculateCAL(groupByJoinClause):
	'''
		Given a data and groupBy table (and associated GroupByJoinClause)
		Will calculate CAL.
	'''
	return """
			SELECT xCoordinate
				,CAL = (CAST(CAL AS real) / totalSN * 100 )
			FROM (
					SELECT	G.id
							,G.xCoordinate
							,totalSN = COUNT(DISTINCT SerialNumberId)
							,CAL = SUM(CASE WHEN calName IS NOT NULL THEN 1.0 ELSE 0.0 END)
					FROM (
						@rawData						--required table
						LEFT JOIN @GroupBy G			--required table
							""" + groupByJoinClause + """
						)	
					GROUP BY id, xCoordinate
				) groupTable
			"""



def calculateRTY(groupByJoinClause):
	'''
		Given a data and groupBy table (and associated GroupByJoinClause)
		Will calculate RTY.
	'''
	return """
			SELECT xCoordinate
					,RTY = AVG(subRTY) * 100
			FROM (	
					SELECT id
							,xCoordinate
							,LineId
							,subRTY = EXP(SUM(LOG(stationRTY))) --Aggregate Multiply using Algebra Magic
					FROM (
						SELECT id
								,xCoordinate
								,LineId
								,StationId
								,stationRTY = CAST((totalSN - allDefects) AS real) / totalSN
						FROM (
							SELECT	G.id
									,G.xCoordinate
									,LineId
									,StationId
									,totalSN = COUNT(DISTINCT SerialNumberId)
									,allDefects = COUNT(DISTINCT CASE WHEN DefectId IS NOT NULL THEN SerialNumberId ELSE NULL END)
										
							FROM (
								@rawData						--required table
								LEFT JOIN @GroupBy G			--required table
									""" + groupByJoinClause + """
								)
							GROUP BY G.id, G.xCoordinate, LineId, StationId
							) lineStationTable
		
						) stationRTYTable
					WHERE stationRTY != 0
						AND stationRTY IS NOT NULL
					GROUP BY id, xCoordinate, LineId
				)rtyTable
			WHERE subRTY != 0
				AND subRTY IS NOT NULL
			GROUP BY xCoordinate
			"""


def buildKPICalculator(calTable, groupByTable, groupByJoinClause, rawDataTable, xAxis, filters):
	'''
		Takes CAL Table, Group By Table, Group By Clause, and Raw Data Table.
		--All of these are text, in SQL format.
		
		In SQL, we join the Raw Data Table with the Group By Table.
		This allows us to order the Serial Numbers by those groups.
		The outer-most SELECT joins the SELECTS that calculate fpyCAL and RTY, respectively.
		
		Returns SQL String that will return a table of xCoordinates, FPY, CAL, and RTY.
	'''
	xCoordinateFormat = ''
	timeOrderBy = ''
	if xAxis == 'Time':
		timeOrderBy = ' ORDER BY CONVERT(DATETIME, fpy.xCoordinate) '
		if filters['timeLevel'] == 'Month':
			xCoordinateFormat = "SELECT SUBSTRING(fpy.xCoordinate, 4, 8) AS xCoordinate --monthly"
		if filters['timeLevel'] == 'Day':
			xCoordinateFormat = "SELECT SUBSTRING(fpy.xCoordinate, 1, 11) AS xCoordinate --daily"
		if filters['timeLevel'] == 'Hour':
			xCoordinateFormat = "SELECT CONCAT(SUBSTRING(fpy.xCoordinate, 13, 5), ' ', SUBSTRING(fpy.xCoordinate, 1, 11)) AS xCoordinate --hourly"
	else:
		xCoordinateFormat = 'SELECT fpy.xCoordinate'
		
	
	return calTable + groupByTable + rawDataTable + '\n' + xCoordinateFormat + """
					,ISNULL(FPY, 0) AS FPY
					,ISNULL(RTY.RTY, 0) AS RTY
					,ISNULL(CAL, 0) AS CAL
			FROM (
					""" + calculateFPY(groupByJoinClause) + """
					) fpy
			
			LEFT JOIN (
					""" + calculateCAL(groupByJoinClause) + """
					) cal
					ON fpy.xCoordinate = cal.xCoordinate
				
			LEFT JOIN (
					""" + calculateRTY(groupByJoinClause) + """
					) rty
					ON fpy.xCoordinate = rty.xCoordinate
			""" + timeOrderBy
	
###### Old ########
#def buildSerialNumberQuery(calTable, groupByTable, groupByJoinClause, filterWhereClause):
#	'''
#		Takes CAL table, Group By Table, Group By Clause, and Filtered Where Clause.
#		--All of these are text, in SQL format.
#		
#		In SQL, we select a table of Serial Numbers (filtered by the Where clause).
#		This is joined with the Group By table, so we can order the Serial Numbers by those groups.
#		The outer-most SELECT calculates FPY/CAL and groups by the xCoordinate (and Group By Table id). 
#		
#		Returns SQL String that will return a table of xCoordinates, FPY, and CAL.
#	'''
#	return calTable + groupByTable + """
#	
#			SELECT xCoordinate
#				,FPY = CAST((isTotal - isDefect) AS real) / isTotal * 100
#				,CAL = (CAST(isCAL AS real) / isTotal * 100 )
#			FROM (
#				SELECT	G.id
#						,G.xCoordinate
#						,isTotal = COUNT(DISTINCT SN.SerialNumberId)
#						,isDefect = COUNT(DISTINCT CASE WHEN D.DefectId IS NOT NULL THEN SN.SerialNumberId ELSE NULL END)
#						,isCAL = SUM(CASE WHEN C.calName IS NOT NULL THEN 1.0 ELSE 0.0 END)
#						
#				FROM dbo.SerialNumber SN
#					LEFT JOIN InspectionResult IR
#						ON SN.SerialNumberId = IR.SerialNumberId
#					LEFT JOIN Defect D
#						ON IR.InspectionResultId = D.InspectionResultId
#							AND D.Deleted = 0
#					LEFT JOIN @cal C
#						ON D.Nonconformity = C.calName
#							AND C.calWeight = 1
#					LEFT JOIN Station S
#						ON IR.StationId = S.StationId
#					LEFT JOIN Line L
#						ON S.LineName = L.WhirlpoolLineCode
#					LEFT JOIN @GroupBy G
#				""" + groupByJoinClause + filterWhereClause + """
#				
#				GROUP BY id, xCoordinate
#			) groupTable
#			ORDER BY xCoordinate
#			"""