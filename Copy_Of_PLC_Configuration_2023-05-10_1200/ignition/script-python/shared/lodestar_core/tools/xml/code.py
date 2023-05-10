'''
    Takes an XML with unescaped ampersand and escapes it
    
    Written by: Jon C (Polaris)     - 2021-06-22
'''

import re

class XML:
	"""A class to modify XML before parsing.
	
	Class works on incoming XML before parsing for the purpose of making it parseable by Python.
	"""
	
	def __init__(self,xml):
		"""Constructor for the XML class.
		
		Constructor for the XML class.
		
		Args:
			xml (String or Dictionary): String or dictionary representation of an XML that is well-formed
		"""
		self.xml = xml
		self.newDict = xml
		
	def escapeAmpString(self):
		"""Escapes ampersand within XML.
		
		Escapes all non-escaped ampersands within an XML file.
		"""
		regex = re.compile(r"&(?!amp;|lt;|gt;)")
		return regex.sub("&amp;", self.xml)
		
	def escapeAmpDict(self,newDict):
		"""Escapes ampersand within XML dictionary.
			
		Escapes all non-escaped special character (<,>,",',&) within each value element of dictionary.
		"""
		escapedList = ('<','>','"',"'",'&')
		for key,value in newDict.items():
			if isinstance(value,dict):
				self.escapeAmpDict(value)
			#Avoid non-string types
			elif isinstance(value,str):
				for escapedChar in escapedList:
					if escapedChar in value:
						value = value.replace('&','&amp;')
						value = value.replace("<",'&lt;')
						value = value.replace('>','&gt;')
						value = value.replace('"','&quot;')
						newDict[key] = value.replace("'",'&apos;')
				
		self.newDict = newDict