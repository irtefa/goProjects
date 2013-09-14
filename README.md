#Directions & Examples


#Underlying Architecture

The machine where we are querying from has a masterlist.txt that contains the ip addresses of all the machines we will query including itself. When we want to add a new machine we have to update the masterlist.txt. Similarly, when we want to remove a machine from the system we just delete the ip address of the machine from masterlist.txt.

Each machine in our distributed system has a metadata.txt file that contains it's ip address, name of the machine and the log file it should operate on. The machine also has a logServer that runs a grep command on this log file upon request from our logClient.

we say what machine is producing the logs which helps us debug code for specific processes runnin on specific machines

runs grep on each individual machines on their logs

whichever one finishes grep first, returns the result to the user

we ignore machines that are down, in the worst case (where all machines are down), we return results from our own machine

#Does our system work?

Apart from testing the functionality manually we created unit test to make sure our system works. Our test generates a test log on a remote machine on the fly and runs a grep on it to check if we are retrieving results that matches with our expected results. 

Average query latency for 100MB logs on 4 machines

#Authors
Irtefa, Mohd
Lee, Stephen