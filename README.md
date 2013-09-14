#Directions & Examples
First, make sure every machine in our distributed system is running the server program. Ths server program is located at 'logServer/server.go'. Each machine should also have Golang installed to run the server.

Make sure every machine in the distributed system has a metadata.txt file with any relevant information about the machine written in the file. This file should be located in the same directory as the 'server.go' program.

Now, on any one machine, to query for certain key-value pairs in log files, we must run the program 'logClient/grep_client.go'. To work properly, the client program uses a 'masterlist.txt' file located in the same directory. This file contains a list of the IP addresses of each machine in the system.

TO run properly, the client program asks for two arguments, the first argument being the pattern matching for the key and the second argument for the value.
Ex: 'go run grep_client.go <keyPattern> <valuePattern>'

Note: to ignore any particular pattern in the key or value, use a wildcard statement in the respective argument. Don't use '.*' as the '.' and the '*' symbol in the first position of an argument breaks all other arguments in Golang. As a replacement, use '^.*' as the wildcard statement, as this works well with our program.

After running the client program with the arguments, the client will query the servers, and the response will be printed out on console.

Example queries=
'go run grep_client.go hello world'
'go run grep_client.go ^ERROR$ ^.*Hi.*$

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
##Irtefa, Mohd
##Lee, Stephen
