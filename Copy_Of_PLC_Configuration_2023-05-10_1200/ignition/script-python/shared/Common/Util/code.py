from java.lang import Exception
import arrow
import sys

# The VirtualModule is a hack to allow the stack frame inspection to work.
# https://forum.inductiveautomation.com/t/error-on-sys-modules/6431
#class VirtualModule(object):
#	def __init__(self,name):
#		import sys
#		sys.modules[name]=self
#	def __getattr__(self,name):
#		return globals()[name]
#VirtualModule("__main__")  


def getUser(user):
	"""
		getUser returns the passed in name if provided
					 or the Username of the current user, if there is one
					 or '[Project]' if neither of the above are valid
	"""
	if user is None:
		try:
			user = system.tag.readBlocking("[client]UserName")[0].value
			if user is None:
				user = "[Project]"
		except:
			user = "[Project]"
	return user
	

def getExceptionCauseString(err):
	"""
		getExceptionCauseString returns the messages from up to 100 nested exceptions
	"""
	i = 0
	message = err.message
	while i < 100 and "cause" in dir(err) and err.cause != None:
		i += 1
		err = err.cause
		message += ';\n' + err.message
	
	exc_type, exc_obj, tb = sys.exc_info()
	f = tb.tb_frame
	lineno = tb.tb_lineno
	filename = f.f_code.co_filename
	return message + ' on line ' + str(lineno) + ' in file ' + filename
	# return message


def debugPrint(*args):
	logger = system.util.getLogger("debugPrint")
	# system.util.setLoggingLevel("debugPrint", "debug")
	message = ""
	for arg in args:		
		message += str(arg) + " "
	logger.debug(message)


def logInfoPrint(logName, *args):
	logger = system.util.getLogger(logName)
	message = ""
	for arg in args:		
		message += str(arg) + " "
	logger.info(message)

def log (level,logName, *args):
	logger = system.util.getLogger(logName)
	message = ""
	for arg in args:		
		message += str(arg) + " "
	if level == 'trace':
		logger.trace(message)
	elif level == 'debug':
		logger.debug(message)
	elif level == 'error':
		logger.error(message)
	elif level == 'warn':
		logger.warn(message)
	else:
		logger.info(message)
	
def logInfoFormat(logName, formatString, *args):
	logger = system.util.getLogger(logName)
	logger.infof(formatString, *args)	

	
		
def insertImage(component,image):
	from javax.swing import ImageIcon
	from java.io import ByteArrayInputStream
	from javax.imageio import ImageIO
	from java.awt import Image
	from java.net import URL
	from java.io import File
	bais = ByteArrayInputStream(image)
	bImageFromConvert = ImageIO.read(bais)
	imageicon = component
	boundWidth = imageicon.width
	boundHeight = imageicon.height
	originalWidth = bImageFromConvert.width
	originalHeight = bImageFromConvert.height
	newWidth = originalWidth
	newHeight = originalHeight
	if originalWidth > boundWidth:
		newWidth = boundWidth
		newHeight = (newWidth * originalHeight) / originalWidth
	if newHeight > boundHeight:
		newHeight = boundHeight
		newWidth = (newHeight * originalWidth) / originalHeight
	scaledImage = bImageFromConvert.getScaledInstance(newWidth,newHeight,Image.SCALE_SMOOTH)
	imageicon.setIcon(ImageIcon(scaledImage))

# JGV TODO: The expression needs to be called in the scope of the caller so we can have access to the local variables 
#def debugAssert(expr):
#	logger = system.util.getLogger("debugAssert")
#	# system.util.setLoggingLevel("debugAssert", "debug")
#
#	previous_frame = inspect.currentframe().f_back
#	(filename, line_number, function_name, lines, index) = inspect.getframeinfo(previous_frame)
#	location = ") in function " + function_name + ", on line " + str(line_number) + " of file " + filename
#
#	result = eval(expr)
#	if result == False: 
#		logger.debug("debugAssert failed: (" + str(expr) + location)


def time_this(function):
    def function_wrapper(*args, **kwargs):
        start = arrow.now()
        value = function(*args, **kwargs)
        stop = arrow.now()
        elapsed_time = stop - start
        log('debug', 'Scheduling.ScheduleTrigger.Minute_Trigger.minute_trigger', 
        	"Timer decorator - The function %s took %f seconds" % (function.__name__, elapsed_time.total_seconds()))
        return value

    return function_wrapper
