'''Contains scripts specific to the Overview Displays.Top10.
	
	Created:  JGV - 03-06-2020 - Split off shared functions for web service support
	Updated:  WJF - 2021-08-05 - Added groups.  Removed shift.
				  - 2021-08-09 - Put shift back in.
'''


from shared.RTY.General import getRTYDb, getLodestarDb
from shared.RTY.OverviewDisplays.RTYandFPY import buildLineShiftTable

def getTop10Defects(areaName='All', lineId=0, stationTypeId=0, stationId=0, platformId=0, componentLocationName='All', responsibilityId=0, groupName='All', shift=False):
	'''
		DocStrings!
	'''
	if areaName in ['All', 'Todo']:
		areaName = None
	if componentLocationName in ['All', 'Todo']:
		componentLocationName = None
	if groupName in ['All', 'Todo']:
		groupName = None
	
	filters = {'areaName': areaName,
				'lineId': lineId,
				'stationTypeId': stationTypeId,
				'stationId': stationId,
				'platformId': platformId,
				'componentLocationName': componentLocationName,
				'responsibilityId': responsibilityId,
				'groupName': groupName}
			
	whereClause, filterVariables = buildWhereClause(filters)
	query = buildTop10SQL(whereClause, shift)
	
	if shift:
		query = buildLineShiftTable() + query
	
	return system.db.runPrepQuery(query, filterVariables, getRTYDb())


def buildWhereClause(filters={}):
	'''
		DocString!
	'''
	filterVariables = []
	
	if filters['areaName']:
		areaClause = " AND S.AreaName = ? "
		filterVariables.append(filters['areaName'])
	else:
		areaClause = ""
	
	if filters['lineId']:
		lineClause = " AND L.LineId = ? "
		filterVariables.append(filters['lineId'])
	else:
		lineClause = ""
	
	if filters['stationTypeId']:
		stationTypeClause = " AND S.StationType = ? "
		filterVariables.append(filters['stationTypeId'])
	else:
		stationTypeClause = ""
			
	if filters['stationId']:
		stationClause = " AND S.StationId = ? "
		filterVariables.append(filters['stationId'])
	else:
		stationClause = ""

	if filters['platformId']:
		platformClause = " AND IR.PlatformId = ? "
		filterVariables.append(filters['platformId'])
	else:
		platformClause = ""

	if filters['componentLocationName']:
		locationClause = " AND CL.ComponentLocationName = ? "
		filterVariables.append(filters['componentLocationName'])
	else:
		locationClause = ""
	
	if filters['responsibilityId']:
		responsibilityClause = " AND D.ResponsibilityId = ? "
		filterVariables.append(filters['responsibilityId'])
	else:
		responsibilityClause = ""
	
	if filters['groupName']:
		groupClause = " AND dbo.fn_GetGroup(L.WhirlpoolLineCode, IR.InspectionResultTimestamp) = ? "
		filterVariables.append(filters['groupName'])
	else:
		groupClause = ""
		
	whereClause = "AND SN.Test = 0 " + areaClause + lineClause + stationTypeClause + stationClause + platformClause + locationClause + responsibilityClause + groupClause
	return whereClause, filterVariables


def buildTop10SQL(whereClause, shift):
	'''
		DocStrings!
	'''
	query = """
			SELECT TOP 10 CONCAT(D.DefectDetailName, ' - ', D.DefectCodeName, ' - ', CL.ComponentLocationName) AS 'Defect'
							,COUNT(CONCAT(D.DefectDetailId, ' - ', D.DefectCodeId)) AS DefectCount
			FROM Defect D
				JOIN ComponentLocation CL
					ON D.ComponentLocationId = CL.ComponentLocationId
				JOIN InspectionResult IR
					ON D.InspectionResultId = IR.InspectionResultId
				JOIN Station S
					ON IR.StationId = S.StationId
				JOIN Line L
					ON S.LineName = L.WhirlpoolLineCode
				LEFT JOIN SerialNumber SN
					ON IR.SerialNumberId = SN.SerialNumberId
			"""
					
	if shift:
		query += """
				LEFT JOIN @lineShift LS
					ON L.WhirlpoolLineCode = LS.WhirlpoolLineCode
		
			WHERE LS.shiftStart < D.DefectTimestamp
				"""
	else:
		query += """
				WHERE DATEADD(HOUR, -1, GETDATE()) < D.DefectTimestamp
			"""
		
	query += whereClause	
	query += """
			GROUP BY CONCAT(D.DefectDetailName, ' - ', D.DefectCodeName, ' - ', CL.ComponentLocationName)
			ORDER BY DefectCount DESC
			"""
	return query