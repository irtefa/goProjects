from time import gmtime, strftime
import random
import time

counts = 250000
messages = [
		'MESSAGE send me a reply',
		'DEBUG who needs to debug?',
		'WARNING your computer is going to blow up',
		'EXCEPTION or is it?',
		'ERROR is a five letter word'
		]

i = 0
while i < counts:
	random.shuffle(messages)
	print(messages[0], end=""), print(strftime(" %a, %d %b %Y %X", gmtime()))
	i+=1
