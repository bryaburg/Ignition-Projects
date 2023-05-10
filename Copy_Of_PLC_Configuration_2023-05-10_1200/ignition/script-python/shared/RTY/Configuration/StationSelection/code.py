'''
	Station Selection Scripts
	
	Created:  JGV		03-06-2020	Split off shared functions for web service support
	
	Updated: 
'''

from shared.RTY.General import getRTYDb
from shared.Common.Db import execSP, getSPResults


def getAreaButtons():
	'''
		Returns pyDataSet of button names (Areas) and zeros.
	'''
	sqlQuery = '''
				SELECT DISTINCT AreaName [BtnName], 0 [Station]
				FROM Station
				WHERE Deleted = 0 
				'''
	return system.db.runQuery(sqlQuery, getRTYDb())


def getLineButtons(areaName):
	''' 
		Takes a string Area Name.
		Returns pyDataSet of button names (Lines) and zeros.
	'''
	sqlQuery = '''
			SELECT DISTINCT LineName [BtnName], 0 [Station]
			FROM Station
			WHERE Deleted = 0 and AreaName = ? 
			'''
	return system.db.runPrepQuery(sqlQuery, [areaName], getRTYDb())


def getStationButtons(areaName, lineName):
	''' 
		Takes strings of the area and line names.
		Returns pyDataSet of button names (Stations) and station ids.
	'''
	sqlQuery = '''
				SELECT DISTINCT StationName [BtnName], StationId [Station]
				FROM Station
				WHERE Deleted = 0 and AreaName = ? and LineName = ?
				'''
	return system.db.runPrepQuery(sqlQuery, [areaName, lineName], getRTYDb())


def insertClientDevice(computerName, macAddress, stationID):
	''' 
		Takes strings computerName and macAddress, int station ID.
		Inserts into Client Device database table.
	'''
	storedProc = system.db.createSProcCall('uspInsertAndUpdateClientDevice')
	inParameters = {'computer_name': computerName,
					'mac_address': macAddress,
					'station_id': stationID}
	types = {'station_id': system.db.INTEGER}
	execSP(storedProc, inParameters, {}, types)
	

def getStationInfo(stationID):
	'''  
		Takes an int station id.
		Returns single row dataset of station information.
	'''
	storedProc = system.db.createSProcCall('uspGetStationInfo')
	inParameters = {'station_id': stationID}
	types = {'station_id': system.db.INTEGER}
	
	return getSPResults(storedProc, inParameters, {}, types)




