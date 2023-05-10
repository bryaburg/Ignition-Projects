'''Contains scripts specific to the Reports.Unit History window.
	
	Created: JGV		03-06-2020 - Split off shared functions for web services
	Updated: WJF		2020-03-23 - Rewrote SQL calls in conjunction with
									 change in Unit History's core design.
			 WJF		2020-05-26 - Added timeDimension and related logic to 
									 getTop10DefectCounts and getTopDefects.
'''


from shared.RTY.General import getRTYDb, datesSelectedAndInRange

def buildWhereClause(filters={}):
	'''
		DocString!
	'''
	filterVariables = []
	
	if filters['startDate'] and filters['endDate']:
		formatForSQL = 'yyyy-MM-dd HH:mm:ss'
		startDateFormat = system.date.format(filters['startDate'], formatForSQL)
		endDateFormat = system.date.format(filters['endDate'], formatForSQL)
		
		dateClause = " AND CreatedOn BETWEEN ? AND ? "
		filterVariables.append(startDateFormat)
		filterVariables.append(endDateFormat)
	else:
		dateClause = ""
	
	if filters['modelName']:
		modelClause = " AND ModelNumber = ?"
		filterVariables.append(filters['modelName'])
	else:
		modelClause = ""
		
	if filters['serialName']:
		serialClause = " AND SerialNumberName = ?"
		filterVariables.append(filters['serialName'])
	else:
		serialClause = ""

#	if filters['stationId']:
#		stationClause = " AND S.StationId = ?"
#		filterVariables.append(filters['stationId'])
#	else:
#		stationClause = ""

	
	whereClause = 'WHERE 1=1 ' + dateClause + modelClause + serialClause 
	return whereClause, filterVariables


def getUnitData(modelName, serialName, startDate=None, endDate=None, timeDimension=-1):
	'''<short description>
										
	<long description>
	
	Args:
		<arg name> (<arg type>): description
	
	Returns:
		<return type>: description
	'''
	if not datesSelectedAndInRange(timeDimension, [startDate, endDate]):
		return system.dataset.toDataSet([],[])  #return a blank dataset
	
	filters = {'modelName': modelName,
				'serialName': serialName,
				'startDate': startDate,
				'endDate': endDate}

	whereClause, queryParams = buildWhereClause(filters)
	
	query = """
			SELECT *
	  		FROM [dbo].[vw_UnitHistory]
			""" + whereClause
			
	return system.db.runPrepQuery(query, queryParams, getRTYDb())
	

def checkModelNameExists(modelName):
	query = """
		SELECT ModelName
		FROM Model
		WHERE ModelName = ?
		"""
	return system.db.runScalarPrepQuery(query, [modelName], getRTYDb())


def flipSerialNumberTestBit(user, serialNumberId):
	'''
		Flips the Test Unit bit of a Serial Number.
	'''
	query = """
			DECLARE @current_test_state bit
			
			SELECT @current_test_state = ISNULL(Test, 0)
			FROM SerialNumber
			WHERE SerialNumberId = ?
				
			UPDATE SerialNumber
			SET Test = ~@current_test_state
				,UpdatedBy = ?
				,UpdatedOn = GETDATE()
			WHERE SerialNumberId = ?
			"""
	queryVariables = [serialNumberId, user, serialNumberId]
	system.db.runPrepUpdate(query, queryVariables, getRTYDb())


