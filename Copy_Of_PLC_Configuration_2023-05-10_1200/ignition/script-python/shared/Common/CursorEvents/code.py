from java.awt import Cursor


###########################################################
###########################################################
def showCrossHair(event):
	cursor = Cursor.getPredefinedCursor(Cursor.CROSSHAIR_CURSOR)
	event.source.cursor = cursor
###########################################################
###########################################################
def showDefault(event):
	cursor = Cursor.getPredefinedCursor(Cursor.DEFAULT_CURSOR)
	event.source.cursor = cursor
###########################################################
###########################################################
def showHand(event):
	cursor = Cursor.getPredefinedCursor(Cursor.HAND_CURSOR)
	event.source.cursor = cursor
###########################################################
###########################################################
def showMove(event):
	cursor = Cursor.getPredefinedCursor(Cursor.MOVE_CURSOR)
	event.source.cursor = cursor
###########################################################
###########################################################
def showCText(event):
	cursor = Cursor.getPredefinedCursor(Cursor.TEXT_CURSOR)
	event.source.cursor = cursor
###########################################################
###########################################################
def showWait(event):
	cursor = Cursor.getPredefinedCursor(Cursor.WAIT_CURSOR)
	event.source.cursor = cursor