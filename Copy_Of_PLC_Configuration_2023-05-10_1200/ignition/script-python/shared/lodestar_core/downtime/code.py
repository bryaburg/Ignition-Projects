def get_uncoded_downtime_cached(line, workcenters=[], include_microstops=False):
	tag = 'Downtime/Ln%sDowntime'% line[-2:]
	downtime = system.tag.readBlocking(tag)[0].value #Get the cached downtime from the past 24hrs for specified line
	microstop = shared.lodestar_core.config.get_production_parameter(line, None, None, 'MICROSTOP')

	downtime = system.dataset.filterColumns(downtime, [
		'Line State Event Begin',
		'Line State Event End', 
		'Line Downtime Equipment Path',
		'Line Downtime Equipment Name',
		'Line Downtime Reason',
		'Line State Duration',
		'Line State Value',
		'Equipment Note',
		'Line Downtime State Time Stamp',
		'Line Downtime End State Time Stamp'
	])
	
	downtime = system.dataset.toPyDataSet(downtime)
	
	events = []
	uncoded_states = [1, 2, 4, 8]
	filtered_by_workcenter = 0
	filtered_by_microstop = 0
	
#	for row in workcenters:
#		print row
	
	for row in downtime:
		#  and not row['Line State Event End'] == None <- This will prune open downtime events
		
		#print "Uncoded State: %s" % str((row['Line State Value'] in uncoded_states or (row['Line State Value'] > 1000 and row['Equipment Note'] == None)))
		#print "Open Downtime Evnt: %s" % str(not row['Line State Event End'] == None)
		
		# This will filter out events that are in an uncoded state or outside the range of reasoncodes
		if (row['Line State Value'] in uncoded_states or (row['Line State Value'] > 1000 and row['Equipment Note'] == None)) and not row['Line State Event End'] == None: 
			# Only prune the workcenters if workcenters are passed in
			
			#print "Workcenters Len: %s" % str(len(workcenters))
			#print "Workcenters > 0: %s" % str(len(workcenters) > 0)
			#print "Path: %s" % str(row['Line Downtime Equipment Path'])
			#print "Path in Workcenters: %s" % str(not (row['Line Downtime Equipment Path'] in workcenters))

			if not (row['Line Downtime Equipment Path'] in workcenters):
				filtered_by_workcenter = filtered_by_workcenter + 1
				continue
			
#			print "not include_microstop: %s" % str(not include_microstops)
#			print "microstop: %s" % str(microstop)
#			print "even duration: %s" % row['Line State Duration']
#			print "%s < %s = %s" % (str(row['Line State Duration']), str(microstop), str(row['Line State Duration'] < microstop))
			
			# Only prune based on microstop threshold if include_microstops is False
			if not include_microstops and (float(row['Line State Duration']) < float(microstop)/60.0):
#				print "Microsotp %s Duration %s" % (float(row['Line State Duration']), float(microstop))
				filtered_by_microstop = filtered_by_microstop + 1
				continue

			event = [row['Line State Event Begin'], 
					row['Line State Event End'],
					system.date.format(row['Line State Event Begin'],"yyyy/MM/dd"),
					system.date.format(row['Line State Event Begin'],"HH:mm:ss"),
					system.date.format(row['Line State Event End'],"HH:mm:ss") if row['Line State Event End'] != None else '',
					row['Line Downtime Equipment Path'],
					row['Line Downtime Equipment Name'],
					row['Line Downtime Reason'],
					'%.2f'%row['Line State Duration'],
					row['Line State Value'],
					row['Line Downtime State Time Stamp'],
					row['Line Downtime End State Time Stamp']]
			events.append(event)
	
	headers = ['FullBeginDate','FullEndDate', 'Date', 'Begin', 'End', 'FullPath', 'Workstation', 'Reason','Duration','State','RecordDate','RecordEndDate']
	#Rebuild the dataset for the display
	
	dataset = system.dataset.toDataSet(headers, events)
	dataset = system.dataset.sort(dataset, 'FullBeginDate',0)
	
#	print "after-filtered downtime: %s" % str(dataset.getRowCount())
#	print "filtered by workcenter: %s" % str(filtered_by_workcenter)
#	print "filtered by microstop: %s" % str(filtered_by_microstop)
	return dataset

def get_state_data(path, uuid):
	headers = ['equipPath', 'stateName', 'stateCode', 'stateType','UUID','isVisible']
	new_data = []
    
	try:  
		if path != '':
			#get all state options
			data = system.mes.getEquipmentStateOptions(path, uuid, "")
			
			#loop through data to filter out equipment state class 
			for item in data:
				stateName = item.getName()
				if item.getMESObjectType().getName() == 'EquipmentStateClass':
					pass
				else:
					#Pull out data and format the row of the data
					stateCode = item.getStateCode()
					stateType = item.getStateTypeName()
					UUID = item.getUUID()
					isVisible = 1
					
					new_data.append([path, stateName, stateCode, stateType,UUID,isVisible])
					
		#add the end row
#		length = len(new_data)
#		if length <= 8:
#			n = 8-length
#			for x in range(n):
#				new_data.append(['', '', 999999, '','',0])
	except:
		pass
		
	eqStates = system.dataset.toDataSet(headers, new_data)
	eqStatesSort = system.dataset.sort(eqStates,2,True)
	
	return eqStatesSort
	


def get_last_5(workcenters):
	results = shared.lodestar_core.utilities.run_named_query('assembly/get_last_5_reasons', {'WORKCENTERS': workcenters})
	return results

def get_group_uuid(path):
	if path != '':
		data = system.mes.getEquipmentStateOptions(path, "", "")
		for item in data:
			stateName = item.getName()
			if stateName == "Groups":
				return item.getUUID()
				
		return '' 

def get_states(path, uuid = ""):
	if uuid == "":
		uuid = get_group_uuid(path)
		
	states = get_state_data(path, uuid)
	return states
	
def split_event(note, user, state, dt_orig, path, wc_path, s_path, split_time ,zero_time):
	#Create Zero Event 1 second before split
	#Add the user at the start time of the code.
	try:
		system.mes.addTagCollectorValue(wc_path, 'Equipment Additional Factor', 'LoggedUser', zero_time, '')
	except:
		try:
			system.mes.updateTagCollectorValue(wc_path, 'Equipment Additional Factor', 'LoggedUser', zero_time, '')
		except:
			pass
	try:
		system.mes.addTagCollectorValue(s_path, 'Equipment Additional Factor', 'LoggedUser', zero_time, '')
	except:
		try:
			system.mes.updateTagCollectorValue(s_path, 'Equipment Additional Factor', 'LoggedUser', zero_time, '')
		except:
			pass
		
		
	#Set the DT original state
	try:
		system.mes.addTagCollectorValue(wc_path, 'Equipment Additional Factor', 'DT Orig', zero_time, '')
	except:
		try:
			system.mes.updateTagCollectorValue(wc_path, 'Equipment Additional Factor', 'DT Orig', zero_time, '')
		except:
			pass
		
	try:
		system.mes.addTagCollectorValue(s_path, 'Equipment Additional Factor', 'DT Orig', zero_time, '')
	except:
		try:
			system.mes.updateTagCollectorValue(s_path, 'Equipment Additional Factor', 'DT Orig', zero_time, '')
		except:
			pass

	#Set the state 		
	try:
		system.mes.addTagCollectorValue(path, 'Equipment State', '', zero_time, 0)
	except:
		try:
			system.mes.updateTagCollectorValue(path, 'Equipment State', '', zero_time, 0)
		except:
			pass
	
	#Create split event 	
	#Update Note
	if note is None:
		note = ''
	system.mes.updateTagCollectorValue(wc_path, 'Equipment Note', '', split_time, note)
	try:
		system.mes.updateTagCollectorValue(s_path, 'Equipment Note', '', split_time, note)
	except:
		pass
	system.mes.updateTagCollectorValue(path, 'Equipment Note', '', split_time, note)

		
	#Add the user at the start time of the code.
	try:
		system.mes.addTagCollectorValue(wc_path, 'Equipment Additional Factor', 'LoggedUser', split_time, user)
	except:
		try:
			system.mes.updateTagCollectorValue(wc_path, 'Equipment Additional Factor', 'LoggedUser', split_time, user)
		except:
			pass
	try:
		system.mes.addTagCollectorValue(s_path, 'Equipment Additional Factor', 'LoggedUser', split_time, user)
	except:
		try:
			system.mes.updateTagCollectorValue(s_path, 'Equipment Additional Factor', 'LoggedUser', splitTime, user)
		except:
			pass
			
	#Set the DT Original state
	if dt_orig is None:
		dt_orig = ''
	try:
		system.mes.addTagCollectorValue(wc_path, 'Equipment Additional Factor', 'DT Orig', split_time, dt_orig)
	except:
		try:
			system.mes.updateTagCollectorValue(wc_path, 'Equipment Additional Factor', 'DT Orig', split_time, dt_orig)
		except:
			pass
		
	try:
		system.mes.addTagCollectorValue(s_path, 'Equipment Additional Factor', 'DT Orig', split_time, dt_orig)
	except:
		try:
			system.mes.updateTagCollectorValue(s_path, 'Equipment Additional Factor', 'DT Orig', split_time, dt_orig)
		except:
			pass

	# Set the State  		
	try:
		system.mes.addTagCollectorValue(path, 'Equipment State', '', split_time, state)
	except:
		try:
			system.mes.updateTagCollectorValue(path, 'Equipment State', '', split_time, state)
		except:
			pass
			
			
def save_event():
	pass