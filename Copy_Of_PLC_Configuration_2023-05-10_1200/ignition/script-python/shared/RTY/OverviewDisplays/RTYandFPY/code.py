'''
	Created by/on:  JGV		03-06-2020	Split off shared functions for web service support
	
	Contains scripts specific to the Overview Displays.RTY and FPY window.
	
	Updated by/on:  
'''


from shared.RTY.General import getRTYDb, getLodestarDb
from shared.RTY.Reports.KPIMetrics import buildGroupBySQL, calculateFPY, calculateRTY


def calcRTYAndFPYByShift(areaName=None, lineName=None):
	'''
		Collects the user input and SQL strings.
		Combines them and sends the final string to SQL.
		Returns pyDataSet of FPY/RTY per Line for the current shift.
	'''
	if areaName in ['All', 'Todo']:
		areaName = None
	if lineName in ['All', 'Todo']:
		lineName = None
	
	filters = {'areaName': areaName,
				'lineName': lineName}
	
	groupBy, groupByClause = buildGroupBySQL('Line', {})
	lineShift = buildLineShiftTable
	filterWhereClause, filterVariables = buildFilterWhereClause(filters)
	rawData = buildRawDataTable(filterWhereClause)
	finalSQL = combineSQL(groupBy, groupByClause, rawData)
	
	return system.db.runPrepQuery(finalSQL, filterVariables, getRTYDb())


def buildLineShiftTable():
	'''
		Returns SQL-formatted string that will fetch LineCodes and ShiftStarts for the current shift.
	'''
	return """
			DECLARE @lineShift table(WhirlpoolLineCode nvarchar(10), shiftStart datetime)
			
			INSERT INTO @lineShift
				SELECT L.WhirlpoolLineCode, dbo.fn_GetCurrentShiftStartTime(L.WhirlpoolLineCode)
				FROM LINE L
			"""


def buildFilterWhereClause(filters):
	'''
		Takes a dictionary of all the filters.
		Returns a SQL Where Clause built from these filters.
	'''
	filterVariables = []
	
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
		
	whereClause =  """
					WHERE IR.InspectionResultId IS NOT NULL
						AND LS.shiftStart IS NOT NULL
						AND ISNULL(D.CreatedOn, IR.InspectionResultTimestamp) > LS.shiftStart	-- static shift filter
						AND SN.Test = 0 """ + areaClause + lineClause
	return whereClause, filterVariables


def buildRawDataTable(filterWhereClause):
	'''
		Takes Filtered Where Clause and applies it to a raw SQL table.
		The calculations and groupings are run on this rawData table.
	'''
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
							,shiftStart datetime
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
						,LS.shiftStart
									
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
					LEFT JOIN @lineShift LS
						ON L.WhirlpoolLineCode = LS.WhirlpoolLineCode
			""" + filterWhereClause

def combineSQL(groupBy, groupByJoinClause, rawData):
	'''
		Returns the FPY/RTY-by-line-for-current-shift SQL string.
	'''
	return groupBy + buildLineShiftTable() + rawData + """
			SELECT fpy.xCoordinate
					,ISNULL(FPY, 0) AS FPY
					,ISNULL(RTY, 0) AS RTY
			FROM ( 
					""" + calculateFPY(groupByJoinClause) + """
				) fpy
			LEFT JOIN(
					""" + calculateRTY(groupByJoinClause) + """
				) rty
			ON fpy.xCoordinate = rty.xCoordinate
			"""
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	