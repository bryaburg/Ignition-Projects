'''
	Created by/on:    JGV		03-06-2020		Split off shared functions for web services
	
	Contains scripts specific to the Reports.Escapes screen.
	
	Updated by:
'''

from shared.RTY.General import getRTYDb, getLodestarDb
			
def getFilteredEscapes(areaName=None, lineName=None, shiftNumber=None, startDate=None, endDate=None):
	'''
		Called in expression bindings to the data tables of both
		LineOnly and LineAndShift.
		This is why Shift specific where clause is separated from whereClause.
	'''
	if areaName in ['All', 'Todo']:
		areaName = None
	if lineName in ['All', 'Todo']:
		lineName = None

	filters = {'areaName': areaName,
				'lineName': lineName,
				'shift': shiftNumber,
				'startDate': startDate,
				'endDate': endDate}
	whereClause, filterVar = buildFilterWhereClause(filters)	
	
#	r1Flag = system.tag.read("Site/Configuration/EnableRTYCheck")
#	if r1Flag and r1Flag.value:	
#		detailQuery = buildDetailQueryR1(whereClause)
#		summaryQuery = buildSummaryQueryR1(whereClause)
#	else:
	detailQuery = buildDetailQuery(whereClause)
	summaryQuery = buildSummaryQuery(whereClause)
		
	details = system.db.runPrepQuery(detailQuery, filterVar, getRTYDb())
	summary = system.db.runPrepQuery(summaryQuery, filterVar, getRTYDb())
	return details, summary


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
	
	if filters['shift']:
		shiftClause = " AND D.Shift = ?"
		filterVariables.append(filters['shift'])
	else:
		shiftClause = ""
			
	if filters['lineName']:
		lineClause = " AND L.LineName = ?"
		filterVariables.append(filters['lineName'])
	else:
		lineClause = ""
		
	whereClause =  " WHERE IR.InspectionResultId IS NOT NULL " + dateClause + areaClause + shiftClause + lineClause
	return whereClause, filterVariables


def buildDetailQuery(filterWhereClause):
	'''
		Docstring
	''' 
	return """
	DECLARE @tblRTYComponents table ([StationId] bigint, [AreaName] nvarchar(50), [LineName] nvarchar(50), [StationName] nvarchar(50), 
								[ComponentLocationId] bigint, ComponentLocationName nvarchar(128), InspectionResultTimestamp datetime)
	
	INSERT INTO @tblRTYComponents 
	SELECT s.[StationId]
	      ,[AreaName]
	      ,[LineName]
	      ,[StationName]
		  ,cl.[ComponentLocationId]
		  ,cl.ComponentLocationName
		  ,ir.InspectionResultTimestamp
	
	  FROM [Station] s
		JOIN [StationType] st ON s.StationType = st.StationTypeId and st.Deleted = 0
		JOIN [StationTypeStyle] sts ON st.StationTypeStyleId = sts.StationTypeStyleId and sts.StationTypeStyleName like '%-Click' 
		JOIN [StationAssignment] sa ON sa.StationId = s.StationId and sa.Deleted = 0
		JOIN [ComponentLocationMap] clm ON sa.ComponentLocationId = clm.ParentLocationId
		JOIN [ComponentLocation] cl ON clm.ChildLocationId = cl.ComponentLocationId 
		JOIN [InspectionResult] ir ON s.StationId = ir.StationId 
		JOIN [Defect] d ON ir.InspectionResultId = d.InspectionResultId and d.Deleted = 0
	
	--SELECT distinct ComponentLocationId FROM @tblRTYComponents 
	
	SELECT 	DefectId
			,ImageId
			,ir.InspectionResultTimestamp
			,s.AreaName
			,WhirlpoolLineCode
			,s.LineName
			,s.StationId
			,s.StationName
			,[Shift]
			--,SubPlatformId
			--,SubPlatformName
			--,ModelName
			--,SerialNumberName
			,ComponentLocationName
			,DefectDetailName
			,DefectCodeName
			,DefectTimestamp
			,Nonconformity
			--,ResponsibilityName
			--,AuditorBadgeNumber
			,Comment
			--,StatusName
			,RepairComment
			,RepairedByBadgeNumber
	FROM @tblRTYComponents rty 
		JOIN Defect d ON rty.ComponentLocationId = d.ComponentLocationId
		JOIN InspectionResult ir ON d.InspectionResultId = ir.InspectionResultId
		JOIN Station s ON ir.StationId = s.StationId
		JOIN Line l ON l.LineName = s.LineName
	""" + filterWhereClause + """ and NOT EXISTS (SELECT [StationId] FROM @tblRTYComponents t WHERE t.StationId = s.StationId)
	ORDER BY DefectTimestamp DESC 
		"""

def buildSummaryQuery(filterWhereClause):
	'''
		Docstring
	''' 
	return """
	DECLARE @tblRTYComponents table ([StationId] bigint, [AreaName] nvarchar(50), [LineName] nvarchar(50), [StationName] nvarchar(50), 
								[ComponentLocationId] bigint, ComponentLocationName nvarchar(128), InspectionResultTimestamp datetime)
	
	INSERT INTO @tblRTYComponents 
	SELECT s.[StationId]
		  ,[AreaName]
		  ,[LineName]
		  ,[StationName]
		  ,cl.[ComponentLocationId]
		  ,cl.ComponentLocationName
		  ,ir.InspectionResultTimestamp
	
	  FROM [Station] s
		JOIN [StationType] st ON s.StationType = st.StationTypeId and st.Deleted = 0
		JOIN [StationTypeStyle] sts ON st.StationTypeStyleId = sts.StationTypeStyleId and sts.StationTypeStyleName like '%-Click' 
		JOIN [StationAssignment] sa ON sa.StationId = s.StationId and sa.Deleted = 0
		JOIN [ComponentLocationMap] clm ON sa.ComponentLocationId = clm.ParentLocationId
		JOIN [ComponentLocation] cl ON clm.ChildLocationId = cl.ComponentLocationId 
		JOIN [InspectionResult] ir ON s.StationId = ir.StationId 
		JOIN [Defect] d ON ir.InspectionResultId = d.InspectionResultId and d.Deleted = 0
	
	--SELECT distinct ComponentLocationId FROM @tblRTYComponents 
	
	SELECT 	s.AreaName
			,WhirlpoolLineCode
			,s.LineName
			,s.StationId
			,s.StationName
			,[Shift]
			,ComponentLocationName
			,COUNT(1) [Escapes]

	FROM @tblRTYComponents rty 
		JOIN Defect d ON rty.ComponentLocationId = d.ComponentLocationId
		JOIN InspectionResult ir ON d.InspectionResultId = ir.InspectionResultId
		JOIN Station s ON ir.StationId = s.StationId
		JOIN Line l ON l.LineName = s.LineName
	""" + filterWhereClause + """ and NOT EXISTS (SELECT [StationId] FROM @tblRTYComponents t WHERE t.StationId = s.StationId)
	GROUP BY s.AreaName
			,WhirlpoolLineCode
			,s.LineName
			,s.StationId
			,s.StationName
			,[Shift]
			,ComponentLocationName
	ORDER BY AreaName, WhirlpoolLineCode, StationName 
		"""
