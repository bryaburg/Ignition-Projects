# TODO: add error handling

def getVisibleTags(rootFolder):
	ds = []
	tags = system.tag.browseTags(rootFolder)
	for tag in tags:
		doc = system.tag.readBlocking(tag.path + ".Documentation")[0].value
		if doc != 'Exclude':
			ds.append([str(tag.path)])
	
	ds = system.dataset.toDataSet(['TagPath'], ds)
	sortedDS = system.dataset.sort(ds, 0)
	return sortedDS
	
	
def getVisibleTags(rootFolder, order):
		ds = []
		tags = system.tag.browseTags(rootFolder)
		for tag in tags:
			doc = system.tag.readBlocking(tag.path + ".Documentation")[0].value
			if doc != 'Exclude':
				limitDict = eval(doc)
				value = limitDict.get('OrderNum')
				ds.append([str(tag.path), value])
		
		ds = system.dataset.toDataSet(['TagPath', 'Order'], ds)
		sortedDS = system.dataset.sort(ds, 1)
		return sortedDS

	
def evalControlLimit(tagPath, limit):
	doc = system.tag.readBlocking(tagPath + ".Documentation")[0].value
	limitDict = eval(doc)
	value = limitDict.get(limit)
	return value

	
def getCavitySizeFromTagPath(tagPath):
	searchPath = tagPath[0: tagPath.rfind('/')]
	
	tags = system.tag.browseTags(searchPath)
	
	serial = ''
	
	for tag in tags:
		if str(tag).endswith("SERIAL_DN") or str(tag).endswith("Non Zero Serial"):
			serial = system.tag.readBlocking(str(tag))[0].value
			if serial != "0":
				break
		
	cavitySize = system.db.runScalarPrepQuery("SELECT CavitySizeInches FROM SerialNumber SN JOIN Part P ON P.PartId = SN.PartId WHERE SerialNumberName = ? AND CavitySizeInches IS NOT NULL ORDER BY SerialNumberId DESC", [serial], "IIoTDev")
	
	##cavitySize = system.db.runScalarPrepQuery("SELECT CavitySizeInches FROM Part WHERE PartId = ?", [partId], "IIoTDev")
	
	if cavitySize is None:
		return 30
	else:
		return cavitySize
		
def authorizeUser(username):
	username = username.upper() + "@na.ad.whirlpool.com"
	
	try:
		user = system.user.getUser("Active Directory", username)
		if user is None:
			return False
		
		roles = user.getRoles()
		
		for role in roles:
			if role.startswith("NA.Ignition.CVD"):
				return True
				
		return False
	except:
		print 'Authorize User exception'
		return False
		
def pingNetworkDevice(deviceIPAddress):
	from java.net import InetAddress
	ip = InetAddress.getByName(deviceIPAddress)
	if ip.isReachable(1000):
		return 1
	else:
		return 0
		
def getIpOfGateway():
	from socket import getaddrinfo
	from re import search

	gatewayAddrInfo = system.tag.read("[System]Client/Network/GatewayAddress")
	gatewayAddr = str(gatewayAddrInfo.getValue())

	addr = search("://(.*):", gatewayAddr)
	addrToSearch =  addr.group(1)
	port = search("com:(.*)/main", gatewayAddr)
	portToSearch = port.group(1)
	ip = getaddrinfo(addrToSearch, portToSearch)
	system.tag.writeBlocking("Alerts_Monitoring/NetworkDeviceMonitoring/GatewayIPAddress", ip[0][3])
	
	return ip[0][3]
		
def translateDataset(data):
	try:
		headers = []
		newRowSet = []
		for col in range(data.columnCount):
			try:
				headers.append(system.util.translate(data.getColumnName(col)))
			except:
				headers.append(data.getColumnName(col))
		for row in range(data.rowCount):
			newRow = []
			for col in range(data.columnCount):
				try:
					translatedTerm = system.util.translate(data.getValueAt(row,col))
					newRow.append(translatedTerm)
				except:
					newRow.append(data.getValueAt(row,col))
					
			newRowSet.append(newRow)
		
		return system.dataset.toDataSet(headers,newRowSet)
	except:
		return None
	
def getRowCount(dataSet):
	rowCount = dataSet.getRowCount()
	return rowCount
