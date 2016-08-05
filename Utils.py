import logging;
import time;


class TimeIt:
	startTime = "";

	@staticmethod
	def start():
		TimeIt.startTime = time.time();

	@staticmethod
	def end():
		logger = Logging.defaults(__name__); # Use this for logging.
		logger.info("TimeIt: %s seconds" %(time.time() - TimeIt.startTime));


class Logging:

	@staticmethod
	def defaults(name, filename="", debugLevel="DEBUG"):
		dl = logging.INFO;
		if debugLevel=="DEBUG":
			dl = logging.DEBUG;
		if filename=="":
			logging.basicConfig(level=dl, format='%(name)-12s: %(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p');
		else:
			logging.basicConfig(level=dl, format='%(name)-12s: %(asctime)s - %(levelname)s - %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', filename=filename);
		return logging.getLogger(name);

	@staticmethod
	def getLogger(name):
		return logging.getLogger("QuaintScience:%s" %(name));

class Misc:

	@staticmethod
	def get(obj, path):
		#print path
		if len(path)==0:
			return obj;
		else:
			if path[0] in obj:
				return Misc.get(obj[path[0]], path[1:])
			else:
				return None;