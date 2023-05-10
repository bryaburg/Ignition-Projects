def execSP(storedProc, inParameters, outParameters, types):
	"""
	execSP - assigns in/out parameters and executes the SP
	storedProc: SProcCall created by a call to system.db.createSProcCall(SP_Name)
			inParameters: dictionary containing values for all incoming SP parameters
			outParameters: dictionary containing values for all default values for outgoing SP parameters
			types: dictionary containing the system.db.<type> for parameters.  Unspecified types are assumed to be system.db.NVARCHAR
	"""
	for p in inParameters:
		if p in types:
			storedProc.registerInParam(p, types[p], inParameters[p])
		else:
			storedProc.registerInParam(p, system.db.NVARCHAR, inParameters[p])
	
	for p in outParameters:
		if p in types:
			storedProc.registerOutParam(p, types[p])
		else:
			storedProc.registerOutParam(p, system.db.NVARCHAR)	  
	
	system.db.execSProcCall(storedProc)	
	
	# Assign outgoing parameter values
	for p in outParameters:
		outParameters[p] = storedProc.getOutParamValue(p)


def getSPResults(storedProc, inParameters, outParameters, types):
	"""
	getSPResults - assigns in/out parameters and executes the SP
	storedProc: SProcCall created by a call to system.db.createSProcCall(SP_Name)
	inParameters: dictionary containing values for all incoming SP parameters
	outParameters: dictionary containing values for all default values for outgoing SP parameters
	types: dictionary containing the system.db.<type> for parameters.
	"""
	
	execSP(storedProc, inParameters, outParameters, types)
	
	results = storedProc.getResultSet()
	return results


def getSPScalar(storedProc, inParameters, outParameters, types):
	"""
	getSPScalar - assigns in/out parameters and executes the SP
	storedProc: SProcCall created by a call to system.db.createSProcCall(SP_Name)
	inParameters: dictionary containing values for all incoming SP parameters
	outParameters: dictionary containing values for all default values for outgoing SP parameters
	types: dictionary containing the system.db.<type> for parameters.  If a non-integer return value is desired, include a "return" type parameter
	"""
	if 'return' in types:
		storedProc.registerReturnParam(types['return'])
	else:
		storedProc.registerReturnParam(system.db.INTEGER)
	
	execSP(storedProc, inParameters, outParameters, types)
	
	results = storedProc.getReturnValue()
	return results
	


def getDistinctByColumnFilter(dataSet, columnName, filterColumnName = None, filterColumnValue = None):
	filterList = []	
	# Shortcut for empty input
	if not dataSet.rowCount:
		return filterList
	
	# Normalize parameters
	if isinstance(filterColumnName, basestring):
		filterColumnNameList = [filterColumnName]
	else:
		filterColumnNameList = filterColumnName
		
	if not hasattr(filterColumnValue, '__iter__'):
		filterColumnValueList = [filterColumnValue]
	else:
		filterColumnValueList = filterColumnValue
			
	if not hasattr(columnName, '__iter__'):
		columnNameList = None
	else:
		columnNameList = columnName
		
	pyData = system.dataset.toPyDataSet(dataSet)
	
	if filterColumnNameList and filterColumnValueList:		
		for row in pyData:
			match = True
			for col in range(len(filterColumnNameList)):				
				if row[filterColumnNameList[col]] != filterColumnValueList[col]:
					match = False
					break
			if match:
				filterList.append(row)
	else:
		for row in pyData:
			filterList.append(row)
	
	resultList = []
	
	if columnNameList:
		for row in filterList:
			newRow = []
			for colName in columnNameList:
				newRow.append(row[colName])
			if not newRow in resultList:
				resultList.append(newRow)
	else:
		for row in filterList:
			newValue = row[columnName]
			if not newValue in resultList:
				resultList.append(newValue)
	
	return resultList

