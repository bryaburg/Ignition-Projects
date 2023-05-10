from java.net import Socket, InetSocketAddress
from java.io import DataOutputStream

def send_tcp(ip, port, payload):
	payload = payload.encode('utf-8')	
	printer_tcp = Socket()
	
	try:	
		printer_tcp.connect(InetSocketAddress(ip, port), 2000)
		printer_output = DataOutputStream(printer_tcp.getOutputStream())
		printer_output.write(payload)
		
		printer_output.close()
		printer_tcp.close()
	except Exception, e:
		print(str(e))
		printer_tcp.close()
		
		
def send_http(ip, payload):
	url = 'http://{{IP}}/pstprnt'.replace('{{IP}}', ip)
	system.net.httpPost(url, postData=payload)