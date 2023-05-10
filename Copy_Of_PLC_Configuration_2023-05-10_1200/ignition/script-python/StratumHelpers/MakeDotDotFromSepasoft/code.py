import re

#helper function for output to let you do what you want with it
def cout(thing):
	print thing


def makeDotDotFromFile(lineName):
	
	# The header for the dot
	header = '''
	digraph G {
		ratio = "fill";
		#size = "4,3";
	    
		rankdir = TB;
		
		subgraph cluster_line {
			mes_type=Line
			label = "%s";
	        
			subgraph cluster_main_line {
				mes_type=MainLine
	''' % lineName
	
	cout(header)
	
	#Pull the info from the db model replication table 
	query = "select * from modelreplication where line = '%s' and deleted = 0 and OBJ_LEVEL > 2 ORDER BY OBJ_LEVEL ASC" % lineName
	db = system.tag.read('Site/Configuration/IgnitionMES_Extension').value
	
	result = system.db.runQuery(query,db)
	
	sublines = {}
	#Get the equipment
	for ln in result:
		name =  ln['OBJ_NAME']
		if name == 'CAL' or name == 'Repair':
			continue
			
		if name[2] == 'X':
			continue
		if 'MNT' in name:
			continue
			
			
		path = ln['PATH']
		lvl = ln['OBJ_LEVEL']
		parent = ln['PARENT_PATH']
		
		#cout( path, lvl
		
		
		
		#set up sublines
		if lvl == 3:
			sublines[name] = []
			#cout( name
		
		if lvl == 4:
			#cout( name
			pName = parent.split('\\')[-1]
			#cout( name,pName
			sublines[pName].append(name)
			#cout( sublines[pName]
	
	
	#need to sort the sublines
	sublinesSorted = []
	
	for x in sublines:
		sublinesSorted.append(x)
	
	sublinesSorted = sorted(sublinesSorted)
	
	
	#format and cout( the dot graph
	def getSubgraphs(mlc):
		if mlc:
			spacing = '\t\t\t'
		else:
			spacing = '\t\t'
		
		rpat = r'[ -]'
		
		if mlc:
			color = 'red'
		else:
			color = 'blue'
		
		mlcsublines = []
		
		#time to print out the subline block for each subline
		for x in sublinesSorted:
			
			#The workcenter and subline blocks are almost identical so
			# code is being resued with a flag of mlc to make the changes
			
			# Build the workstation list
			if mlc:
				if not 'MLC' in x:
					continue
				mlcsublines.append(x)
			else:
				if 'MLC' in x:
					continue
			
			cout( spacing+ 'subgraph cluster_%s_line {' % re.sub(rpat,'',x))
			
			#Sort the work stations since they will order properly if alphabetical
			work = sorted(sublines[x], key=lambda i: int(i[-2:]))
			#I should proabaly do this outside of this loop, but oh well
			sublines[x] = work
			for ws in work:
				cout( spacing+ '\t"%s"[shape=oval, style=filled, color=%s, mes_type=Workstation];' % (ws,color))
			
			cout( '\t')
			
			connString = ''
			
			if len(work) > 1:
				
				for ws in work:
					addString = '"%s" -> ' % ws
					connString += addString
				cout( spacing+ '\t'+connString[:-4])
			
			cout( '\t')
			
			cout( spacing + '\tlabel = "%s"' % x)
			
			cout( spacing + '\tcolor=black')
		
			if mlc:
				cout( spacing + '\tmes_type=Workcenter')
			else:
				cout( spacing + '\tmes_type=Subline')
			
			cout( spacing + '}')
			
		# Add the interconnects if MLC since we know where those are
		if mlc:
			
			for i in range(0,len(mlcsublines)-1):
				one = sublines[mlcsublines[i]][-1]
				two = sublines[mlcsublines[i+1]][0]
				cout( spacing + '"%s" -> "%s"' % (one,two))
			
			mainLineFooter = '''
			
				label = "Main Line";
			}
			'''
			
			cout( mainLineFooter)
		
	
	getSubgraphs(True)
	getSubgraphs(False)	
	
	# Take a guess at what the interconnections will be.
	# Assume that it goes in Alphabetical order with sublines of the same letter feeding into main line
	
	# make sure that lineInfo has all of the information
	setA = set(sublinesSorted)
	setB = set(sublines.keys())
	
	assert not setA-setB
	assert not setB-setA
	
	
	#group everything with the same starting letter
	subLineNameDict = {}
	subLineNameDictSortedKeys = []
	
	for sublineName in sublinesSorted:
		letter = sublineName[0]
		if letter in subLineNameDict.keys():
			subLineNameDict[letter].append(sublineName)
		else:
			subLineNameDict[letter] = [sublineName]
			subLineNameDictSortedKeys.append(letter)
			
	#deal with sources.  Lets guess that there will be a single source for 
	#	the sublines leading up to the main line and sources for sublines 
	#	with a main line connection
	
	#Assuming the first will never be mainLine
	assert not 'MLC' in sublinesSorted[0]
	
	#(letter,letter,letter,letter,workstation) 
	sourceTemplate = '''
			#%s source
			Store%s [label="Source-%s", shape=house, mes_type=Source, thing_type=Drive];
			Store%s -> "%s"
	''' 
	
	
	# set up the first source
	letter = subLineNameDictSortedKeys[0]
	subNames = subLineNameDict[letter][0]
	firstWorkstation = sublines[subNames][0]
	
	cout(sourceTemplate % (letter,letter,letter,letter,firstWorkstation))
	
	
	# do Magic
	# Now we attempt to guess at the structure of the sublines
	# All sublines go in alphabetical order and flow from one to the other
	# until a MLC workcenter is reached.  Any other non MLC will side feed
	# the main line.
	
	
	# This loop will go through all of the sublines/workstation and process if
	# a connection is needed and/or a source
	
	# Sources and inter subline connections should be grouped so I need to output
	# after processing
	sourcesNeeded = []
	connectionsNeeded = []
	lastMLCName = ''
	
	#Boolean for if the last one was 
	MLCStarted = False  
	for i in range(0, len(subLineNameDictSortedKeys)-1):
		# Get the first subline
		letterA = subLineNameDictSortedKeys[i]
		sublineNamesA = subLineNameDict[letterA]
		sublineNameA = sublineNamesA[0]
		#print sublineNameA
		
		if not MLCStarted:
			#check corner cases for prior to MLC conditions
			if len(sublineNamesA) > 1:
				# we know that this is a subline feeding a mainline from the side
				MLCStarted = True
				sublineNameB = sublineNamesA[1]
				connectionsNeeded.append([sublineNameA,sublineNameB])
				lastMLCName = sublineNameB
				#print 'Connection Added %s to %s' % (sublineNameA,sublineNameB)
				
			elif 'MLC' in sublineNameA:
				#Then thi is taken care of in the main line and can be ignored
				MLCStarted = True
				lastMLCName = sublineNameA
				
			else:
				# normal subline Connection prior to Main. No source needed.
				letterB = subLineNameDictSortedKeys[i+1]
				sublineNameB = subLineNameDict[letterB][0]
				connectionsNeeded.append([sublineNameA,sublineNameB])
		else:
			#Main line has already been started, any non MLC need sources
			
			if 'MLC' in sublineNameA:
				#only need to check for end of MLC
				#lets try and fix this after the fact
				lastMLCName = sublineNameA
			else:
				MLCStarted = False
				sourcesNeeded.append(sublineNameA)
				letterB = subLineNameDictSortedKeys[i+1]
				sublineNameB = subLineNameDict[letterB][0]
				connectionsNeeded.append([sublineNameA,sublineNameB])
	
	# Remove the last source and set up a MLC to subline connection instead
	if len(sourcesNeeded)>0 and not 'MLC' in sublinesSorted[-1]:
		sublineConnection = sourcesNeeded[-1]
		sourcesNeeded = sourcesNeeded[:-1]
		connectionsNeeded.append([lastMLCName,sublineConnection])
	
	# quick comment
	cout('\t\t# subline connections')	
		
	# output the sources so they are together
	for source in sourcesNeeded:
		letter = source[0]
		firstWorkstation = sublines[source][0]
		cout(sourceTemplate % (letter,letter,letter,letter,firstWorkstation))
	
	
	# output the new connections
	connectionTemplate = '''		#%s-%s
			"%s" -> "%s"'''
	
	for conn in connectionsNeeded:
		letterA = conn[0][0]
		letterB = conn[1][0]
		
		workstationA = sublines[conn[0]][-1]
		workstationB = sublines[conn[1]][0]
		
		cout(connectionTemplate % (letterA,letterB,workstationA,workstationB))
	
	
	#Set up output connection
	outputTemplate = '''
		#Sink
		Out [label="Output", shape=invhouse, mes_type=Sink]
	
		# Need to double check subline to out connection.
		#%s to Sink
		"%s" -> Out
	}   
    
}
		'''
	
	lastSubline = sublinesSorted[-1]
	letter = lastSubline[0]
	lastWorkstation = sublines[lastSubline][-1]
	
	cout(outputTemplate % (letter,lastWorkstation))


#makeDotDotFromFile('LN07')



	
