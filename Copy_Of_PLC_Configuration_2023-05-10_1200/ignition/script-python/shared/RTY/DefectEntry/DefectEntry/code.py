'''Defect Entry Template Scripts
	
	Created: JGV		03-06-2020	Split off shared functions for web service support
	
	Updated:  
	WJF - 2021-06-22 - Changed some spacing and comments. 
'''


from shared.Common.Db import getSPResults
from shared.Common.Util import getUser, logInfoPrint
from shared.RTY.General import getRTYDb, getLodestarDb


########################################################################################################
########################################################################################################

def getComponentGroupClassName(componentID):
	query = """
			SELECT ComponentName
			FROM Component
			WHERE ComponentId = ?
			"""
	return system.db.runPrepQuery(query, [componentID], getRTYDb())

########################################################################################################
#Check if everything above this can be depricated when we finish the new defect entry screens.
########################################################################################################
##### Defect Entry Scripts

def getComponentDetail(componentLocationID):
	'''
		Necessary because defect options are looked up by unique Component Names in the R-one repair table.
	'''
	query = """
			SELECT ComponentGroup, ComponentClass, ComponentName, ComponentLocationName
			FROM dbo.ComponentLocation
			WHERE ComponentLocationId = ?
			"""
	componentDefinition = system.db.runPrepQuery(query, [componentLocationID], getRTYDb())

	componentGroup = componentDefinition[0][0]
	componentClass = componentDefinition[0][1]
	componentName = componentDefinition[0][2]
	componentLocationName = componentDefinition[0][3]

	return componentGroup, componentClass, componentName, componentLocationName


def getDefectData(componentName):
	'''
		Get Defect Detail and Code information from Component Name (R-one DB table)
	'''
	query = """
			SELECT defectDetail.ID AS btnId, defectDetail.REPAIR_CODE_NAME AS btnName
					,defectCode.ID AS defectCodeId, defectCode.REPAIR_CODE_NAME AS defectCodeName
			FROM [dbo].[RepairCode] componentName
				JOIN [dbo].[RepairCodeRelations] rcr1
					ON rcr1.PARENT_ID = componentName.ID
						AND componentName.REPAIR_CODE_TYPE = 'ComponentName'
						AND componentName.REPAIR_CODE_NAME = ?
						AND componentName.DELETED = 0
						AND rcr1.DELETED = 0
				JOIN [dbo].[RepairCode] defectCode
					ON rcr1.CHILD_ID = defectCode.ID
						AND defectCode.DELETED = 0
				JOIN [dbo].[RepairCodeRelations] rcr2
					ON rcr2.PARENT_ID = defectCode.ID
						AND rcr2.DELETED = 0
				JOIN [dbo].[RepairCode] defectDetail
					ON rcr2.CHILD_ID = defectDetail.ID
						AND defectDetail.DELETED = 0
			"""
	return system.db.runPrepQuery(query, [componentName], getLodestarDb())

	
def getPlatformsForLine(lineName):
	'''
		Takes a line from the station definition and returns the platform names/ids associated in a dataset.
	'''
	query = """
			SELECT sp.SubPlatformName AS platformName, sp.SubPlatformId as platformId
			FROM [dbo].[Line] l
				JOIN [dbo].[LinePlatformMap] lpm
					ON l.LineId = lpm.LineId
						AND l.LineName = ?
				JOIN [dbo].[Platform] p
					ON lpm.PlatformId = p.PlatformId
						AND p.Deleted = 0
				JOIN [dbo].[SubPlatform] sp
					ON p.PlatformId = sp.PlatformId
			"""

	return system.db.runPrepQuery(query, [lineName], getRTYDb())


def flipDefectDeletedBit(user, defectId):
	'''
		Flips the deleted bit of a Defect
	'''
	query = """
			DECLARE @current_deleted_state bit
			
			SELECT @current_deleted_state = ISNULL(Deleted, 0)
			FROM Defect
			WHERE DefectId = ?
				
			UPDATE Defect
			SET Deleted = ~@current_deleted_state
				,UpdatedBy = ?
				,UpdatedOn = GETDATE()
			WHERE DefectId = ?
			"""
	queryVariables = [defectId, user, defectId]
	system.db.runPrepUpdate(query, queryVariables, getRTYDb())

	
def getNonconformitySelections():
	'''
		Returns a dataset of the Lodestar non-conformity selections
	'''
	query = """
				SELECT ROW_NUMBER() OVER(ORDER BY REPAIR_CODE) AS [btnId], CONCAT(REPAIR_CODE, ' - ', REPAIR_CODE_NAME) AS [btnName]
				FROM [dbo].[RepairCode]
				WHERE REPAIR_CODE_TYPE = 'NonConformity'
			"""
	return system.db.runQuery(query, getLodestarDb())


def getResponsibilitiesForStation(stationID):
	query = """
		SELECT r.ResponsibilityId as btnId, r.ResponsibilityName as btnName
		FROM [dbo].[Station] s
			JOIN [dbo].[StationResponsibility] sr
				ON s.StationId = sr.StationId
				AND s.StationId = ?
			JOIN [dbo].[Responsibility] r
				ON sr.ResponsibilityId = r.ResponsibilityId
		"""
	return system.db.runPrepQuery(query, [stationID], getRTYDb())


def updateDefect(responsibilityId, nonconformity, defectComment, defectCodeId, defectCodeName, defectDetailId, defectDetailName, user, defectId):
	query = """
				UPDATE dbo.Defect
				SET [ResponsibilityId] = ?,
					[Nonconformity] = ?,
					[Comment] = ?,
					[DefectCodeId] = ?,
					[DefectCodeName] = ?,
					[DefectDetailId] = ?,
					[DefectDetailName] = ?,
					[UpdatedBy] = ?,
					[UpdatedOn] = SYSDATETIME()
				WHERE DefectId = ?
			"""
	queryVariables = [responsibilityId,
						nonconformity,
						defectComment,
						defectCodeId,
						defectCodeName,
						defectDetailId,
						defectDetailName,
						user,
						defectId]
	system.db.runPrepUpdate(query, queryVariables, getRTYDb())


def insertNewDefect(inspectionResultId, componentGroup, componentClass, componentName,	
	responsibilityId, shiftNumber, nonconformity, defectComment, 
	componentLocationId, defectCodeId, defectCodeName, defectDetailId, defectDetailName, user):
		defectTimestamp = system.date.now()
		status = 1
		deleted = 0
	
		query = """
				INSERT INTO dbo.Defect
				([InspectionResultId]
				,[ComponentGroup]
				,[ComponentClass]
				,[ComponentName]
				,[ResponsibilityId]
				,[Shift]
				,[Nonconformity]
				,[DefectTimestamp]
				,[Comment]
				,[StatusId]
				,[ComponentLocationId]
				,[DefectCodeId]
				,[DefectCodeName]
				,[DefectDetailId]
				,[DefectDetailName]
				,[CreatedBy]
				,[CreatedOn]
				,[Deleted])
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSDATETIME(), ?)
				"""
		queryVariables = [inspectionResultId, componentGroup, componentClass, componentName,
							responsibilityId, shiftNumber, nonconformity, defectTimestamp, defectComment, status,
							componentLocationId, defectCodeId, defectCodeName, defectDetailId, defectDetailName,
							user, deleted]
		return system.db.runPrepUpdate(query, queryVariables, getRTYDb(), getKey=1)
	

def getCurrentShiftStartTime(currentLineCode):
	'''
		Returns current shift start time for provided whirlpool line code.
	'''
	query = "SELECT dbo.fn_GetCurrentShiftStartTime(?)"
	return system.db.runScalarPrepQuery(query, [currentLineCode], getRTYDb())
		
		
def stationTypeAttributes(stationType):
	'''
		Take StationType.
		Return pyDataSet of stationType attributes.
	'''
	query = """
			SELECT [ShowEditButton]
					,[ShowRepairedButton]
					,[ShowDeleteButton]
					,[PromptBeforeUpdate]
					,[SerialNumberEnabled]
					,[RequiresNewSerial]
					,[AutoScanTimer]
			FROM [StationType] 
			WHERE [StationTypeName] = ?
			"""
	return system.db.runPrepQuery(query, [stationType], getRTYDb())


def getLastToteNumByPlatformStation(platformId):
    query = """
	        SELECT  TOP 1 sn.SerialNumberName
	        FROM [InspectionResult] ir
	        LEFT OUTER JOIN [SerialNumber] sn 
	        ON ir.SerialNumberId = sn.SerialNumberId
	        WHERE ModelNumber='TOTE'
	        AND ir.PlatformId=? 
	        ORDER BY InspectionResultTimestamp DESC                                            
	    	"""
    return system.db.runPrepQuery(query, [platformId], getRTYDb())


def componentLocationAndName(subPlatformId, stationID):
	'''
		Take SubPlatformId and StationId.
		Return pyDataSet of componentLocation Id and Name.
	'''
	query = """
			SELECT CL.[ComponentLocationId]
					,SP.SubPlatformName + ' - ' + CL.ComponentLocationName AS [ComponentName]
			FROM [Station] S
				JOIN StationAssignment SA
					ON S.StationId = SA.StationId
						AND SA.SubPlatformId = ?
				JOIN SubPlatform SP
					ON SP.SubPlatformId = ?
				LEFT JOIN ComponentLocation CL
					ON SA.ComponentLocationId = CL.ComponentLocationId
			WHERE S.[StationId] = ?
			"""
	return system.db.runScalarPrepQuery(query, [subPlatformId, subPlatformId, stationID], getRTYDb())


def getDefectInstanceData(defectId):
	'''
		Takes the id of a defect.
		Returns dataset of raw information on a single defect.
	'''
	query = """
			SELECT *
			FROM dbo.Defect d
				LEFT JOIN dbo.ComponentLocation cl
					ON d.ComponentLocationId = cl.ComponentLocationId
				LEFT JOIN dbo.Responsibility r
					ON d.ResponsibilityId = r.ResponsibilityId
			WHERE DefectId = ?
			"""
	return system.db.runPrepQuery(query, [defectId], getRTYDb())		
		
		
		
		
		
		
		
		
		
		
		
		
