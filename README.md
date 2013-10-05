#Directions & Examples
First, go into udpServer. Inside this folder, there will be a program called 'server.go'. To run this program, type into console "go run server.go 'IP_ADDRESS_OF_CURRENT_MACHINE' 'IP_ADDRESS_OF_CONTACT_POINT". You must pass in the IP addresses of the machine it is running on and the contact point.
After the program is run, type in 'join' to communicate with the contact machine. 
Once the join is successful, the console will log some messages, like START and JOIN.

To leave the network, type 'leave' at any time. This will send a leave request to all other machines in the membership list.

To exit the program, type 'exit' after you pressed 'leave'.

#Underlying Architecture
We wrote a distributed system that implements the gossip protocol. Each machine in our system has a membership list that ideally contains information about all the machines including itself in the system. Each entry in the membership list has the following columns: name of the machine (where name is the the timestamp when the daemon started + # + the ip address of the machine), heartbeat counter, timestamp when the machine’s heartbeat counter was last updated, two boolean flags that keep track of machine failure and leaves. At regular intervals each machine randomly picks ‘k’ other machines in the system from the membership list and sends it’s membership list. Each machine has a seperate go routine running concurrently that updates its membership list when it receives information from other machines in the system. If we observe that a machine’s heartbeat counter has not been updated since 5 s, we mark the member as a failure. In this way, we satisfy the timeliness requirement of 5 s. 

We follow a message format that is platform independent. Before we send a heartbeat we marshal our membership list into an array where each has a key (machine name) and the value is a key-value pairs of heartbeat counter, timestamp, failure-flag and leave-flag and encode it to json. As json is platform independent all the other machines regardless of them being in different platforms can easily decode the message and update their membership lists. 

#Authors
###Irtefa, Mohd
###Lee, Stephen
