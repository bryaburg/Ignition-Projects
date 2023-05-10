import datetime
def downtime(data):

#table is a dataset passed in to be filtered for specific downtims to be displayed.	
	
	#Create a datset for all of the downtime codes that we want to display.
	codes = []
	codes.append([1])
	codes.append([2])
	codes.append([4])
	codes.append([8])
	
	# Get the dataset of workcenters the user has.
	wcFilter = system.tag.readBlocking("[Client]CliWC")[0].value
	wcFilter = system.dataset.toPyDataSet(wcFilter)
	
	#get the columns from the passed in dataset
	columnNames = list(data.getColumnNames())	
	
	#Define the headers.
	headers = ["FullBeginDate","Date","Begin", "End","FullPath","Workstation","Reason","Duration"]
	
	#convert the data to a pydataset
	pds = system.dataset.toPyDataSet(data)
	
	
	nds = []
	for row in pds:
		#Loop through the data filtering out data that is not needed.
		
		#Declare empty dataset
		rowList = []
		#Get column data.
		code = row[6]
		path = row[3]
		#Get just the workcenter out of the equipment path
		d = path.split('\\')		
		wc = d[len(d)-1]
		
		#Build the rowlist dataset with only the columns we care about.
		for column in columnNames:
			if column == "Begin" or column == "End" or column == "Equipment Path" or column == "Equipment Name" or column == "Reason" or column == "Duration":
				rowList.append(row[column])
		 
		#Add rows to the final dataset to be returned	
		
		#only return datasets that have the downtimes we want.		
		for row in codes:		
			if code == row[0]:
				#Only return rows that have a workcenter that is in the chosen workcenters dataset.
				for row in wcFilter:
					w = row[0]
					if w == wc:
						nds.append(rowList)
		
	#Manipulate the data and return just that.	
	rds = []
	for row in nds:
		if row[1] is not None:
			date = system.date.format(row[0], "yyyy/MM/dd")
			start = system.date.format(row[0], "HH:mm:ss")
			end = system.date.format(row[1], "HH:mm:ss")
			duration = str(datetime.timedelta(minutes = row[5]))
			gPath = "[global]\\" + row[2]
			rds.append([row[0],date,start,end,gPath,row[3],row[4],duration])
		
	
	#return the results and assing to a client tag.	
	rds = system.dataset.toDataSet(headers,rds)
	system.tag.writeBlocking("[Client]Downtime Events",rds)
	
##########################################################################
##########################################################################	
	
def Top5(data):

	#get the columns from the passed in dataset
	columnNames = list(data.getColumnNames())	

	#Define the headers.
	headers = ["Value","selectedText","unselectedText","selectedBackground"]


	#convert the data to a pydataset
	pds = system.dataset.toPyDataSet(data)
	
	
	nds = []
	value = 1
	for row in pds:
		#Loop through the data filtering out data that is not needed.
		
		selectedText = row[1]
		unselectedText = row[1]
		selectedBackground = 'color(0,255,0,255)'
		nds.append([value,selectedText,unselectedText,selectedBackground])
		value += 1
		
		
	
		
	
	nds = system.dataset.toDataSet(headers,nds)
	return nds

##########################################################################
##########################################################################	
	
def GetStateData(equipPath,ParentUUID):
		hdr = ['equipPath', 'stateName', 'stateCode', 'stateType','UUID','isVisible']
		newData = []
		try:  
			if equipPath != '':
				data = system.mes.getEquipmentStateOptions(equipPath, ParentUUID, "")
				for item in data:
					stateName = item.getName()
					if item.getMESObjectType().getName() == 'EquipmentStateClass':
						pass
					else:         
						stateCode = item.getStateCode()
						stateType = item.getStateTypeName()
						UUID = item.getUUID()
						isVisible = 1
						newData.append([equipPath, stateName, stateCode, stateType,UUID,isVisible])
			length = len(newData)
			if length <= 8:
				n = 8-length
				for x in range(n):
					newData.append(['', '', 999999, '','',0])
				
					 
			eqStates = system.dataset.toDataSet(hdr, newData) 
			eqStatesSort = system.dataset.sort(eqStates,2,True)
						     
			return eqStatesSort
		except:
			pass
	
##########################################################################
##########################################################################	

def GetGroupUUID(equipPath):  
	GroupUUID = ""	
	if equipPath != '':
		data = system.mes.getEquipmentStateOptions(equipPath, "", "")
		for item in data:
			stateName = item.getName()
			if stateName == "Groups":
				GroupUUID = item.getUUID()
					  
	return GroupUUID

##########################################################################
##########################################################################
		
def GetStates(equipPath,UUID):
	if UUID == "":
		GroupUUID = project.Downtime.GetGroupUUID(equipPath)
		data = project.Downtime.GetStateData(equipPath,GroupUUID)
		return data
	else:
		data = project.Downtime.GetStateData(equipPath,UUID)
		return data


		

	
