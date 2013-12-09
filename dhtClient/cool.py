import subprocess
import time
import sys

SELF_IP, CONTACT_IP, VERSION, DATA  = sys.argv[1:]

class Cool:

    def execute(self, cmd):
        proc = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE)

        for line in proc.stdout:
            print line.rstrip()

    def push(self):
        for i in range(0,100):
            key = str(i)
            value = str(i)
            cmd = self.insert(key, value)
            self.execute(cmd)

    def see(self):
        if DATA:
            limit = 100
        for i in range(0, limit):
            key = str(i)
            cmd = self.lookup(key)
            self.execute(cmd)
    
    def elixir(self):
        if DATA:
            limit = 100
        for i in range(0, limit):
            key = str(i)
            value = "life"
            cmd = self.update(key, value)
            self.execute(cmd)

    def insert(self, key, value):
        return "./dhtClient " + SELF_IP + " " + CONTACT_IP + " "+VERSION+" insert "+ key + " " + value

    def lookup(self, key):
        return "./dhtClient " + SELF_IP + " " + CONTACT_IP + " "+VERSION+" lookup " + key

    def update(self, key, value):
        return "./dhtClient " + SELF_IP + " " + CONTACT_IP + " "+VERSION+" update " + key + " " + value

    def delete(self, key):
        return "./dhtClient " + SELF_IP + " " + CONTACT_IP + " "+VERSION+" delete " + key

c = Cool()


cmd = ""

while cmd != "quit":
    cmd = raw_input().lower()
    if cmd == "push":
        c.push()
    elif cmd == "see":
        c.see()
    elif cmd == "flush":
        c.flush()
    elif cmd == "elixir":
        c.elixir()
    elif cmd == "quit":
        sys.exit()
    else:
        print "INCORRECT COMMAND"
