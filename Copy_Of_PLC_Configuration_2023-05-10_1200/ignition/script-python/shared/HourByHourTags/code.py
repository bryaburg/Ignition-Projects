def movePLCtagToMemoriyTag(tagpath,value):
	indices = [i for i, a in enumerate(tagpath) if a == '/']			
	counterType = tagpath[(indices[2])+1:]
	line = tagpath[(indices[0])+1:(indices[1])]
	
	if counterType == 'Theoretical_Units':
		system.tag.writeBlocking("[State_Assembly]Dashboard/"+line+"/HourByHour/Theoretical_Units Counter",value + 1)
	elif  counterType == 'Units_Produced':
		system.tag.writeBlocking("[State_Assembly]Dashboard/"+line+"/HourByHour/Units_Produced Counter",value + 1)
