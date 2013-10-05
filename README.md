#Directions & Examples
First, go into udpServer. Inside this folder, there will be a program called 'server.go'. To run this program, type into console "go run server.go 'IP_ADDRESS_OF_CURRENT_MACHINE'". You must pass in the IP address of the current machine as a single parameter.
After the program is run, type in 'join' to communicate with the contact machine. NOTE: the contact machine's ip address has been hard-coded into the code as 192.17.11.40. This ip address must be changed to a guaranteed machine that will be alive.
Once the join is successful, the console will log some messages, like START and JOIN.

To leave the network, type 'leave' at any time. This will send a leave request to all other machines in the membership list.

#Authors
###Irtefa, Mohd
###Lee, Stephen
