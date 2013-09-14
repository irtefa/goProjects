import random

counts = 25000000
keys = ['MESSAGE', 'DEBUG', 'WARNING', 'EXCEPTION', 'ERROR']
messages = [
        'send me a reply',
        'who needs to debug?',
        'your computer is going to blow up',
        'or is it?',
        'is a five letter word'
        ]

i = 0
while i < counts:
    random.shuffle(messages)
    random.shuffle(keys)
    print(keys[0] + ":" + messages[0])
    if keys[4] == 'WARNING' and messages[2] == 'or is it?':
        print("RARE:i am a walrus")
    i+=1
