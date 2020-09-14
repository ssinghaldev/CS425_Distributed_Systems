package main

//NETWORK DIAGRAM
//Monitor: [List of nodes that are being monitored]
var MONITOR_NODE_MAPPING = map[string][]string {
    "fa19-cs425-g30-01.cs.illinois.edu": []string{
        "fa19-cs425-g30-09.cs.illinois.edu",
        "fa19-cs425-g30-10.cs.illinois.edu",
        "fa19-cs425-g30-02.cs.illinois.edu",
        "fa19-cs425-g30-03.cs.illinois.edu",
    },
    "fa19-cs425-g30-02.cs.illinois.edu": []string{
        "fa19-cs425-g30-03.cs.illinois.edu",
        "fa19-cs425-g30-04.cs.illinois.edu",
        "fa19-cs425-g30-01.cs.illinois.edu",
        "fa19-cs425-g30-10.cs.illinois.edu",
    },
    "fa19-cs425-g30-03.cs.illinois.edu": []string{
        "fa19-cs425-g30-04.cs.illinois.edu",
        "fa19-cs425-g30-05.cs.illinois.edu",
        "fa19-cs425-g30-01.cs.illinois.edu",
        "fa19-cs425-g30-02.cs.illinois.edu",
    },
    "fa19-cs425-g30-04.cs.illinois.edu": []string{
        "fa19-cs425-g30-05.cs.illinois.edu",
        "fa19-cs425-g30-06.cs.illinois.edu",
        "fa19-cs425-g30-02.cs.illinois.edu",
        "fa19-cs425-g30-03.cs.illinois.edu",
    },
    "fa19-cs425-g30-05.cs.illinois.edu": []string{
        "fa19-cs425-g30-06.cs.illinois.edu",
        "fa19-cs425-g30-07.cs.illinois.edu",
        "fa19-cs425-g30-03.cs.illinois.edu",
        "fa19-cs425-g30-04.cs.illinois.edu",
    },
    "fa19-cs425-g30-06.cs.illinois.edu": []string{
        "fa19-cs425-g30-07.cs.illinois.edu",
        "fa19-cs425-g30-08.cs.illinois.edu",
        "fa19-cs425-g30-04.cs.illinois.edu",
        "fa19-cs425-g30-05.cs.illinois.edu",
    },
    "fa19-cs425-g30-07.cs.illinois.edu": []string{
        "fa19-cs425-g30-08.cs.illinois.edu",
        "fa19-cs425-g30-09.cs.illinois.edu",
        "fa19-cs425-g30-05.cs.illinois.edu",
        "fa19-cs425-g30-06.cs.illinois.edu",
    },
    "fa19-cs425-g30-08.cs.illinois.edu": []string{
        "fa19-cs425-g30-09.cs.illinois.edu",
        "fa19-cs425-g30-10.cs.illinois.edu",
        "fa19-cs425-g30-06.cs.illinois.edu",
        "fa19-cs425-g30-07.cs.illinois.edu",
    },
    "fa19-cs425-g30-09.cs.illinois.edu": []string{
        "fa19-cs425-g30-10.cs.illinois.edu",
        "fa19-cs425-g30-01.cs.illinois.edu",
        "fa19-cs425-g30-07.cs.illinois.edu",
        "fa19-cs425-g30-08.cs.illinois.edu",
    },
    "fa19-cs425-g30-10.cs.illinois.edu": []string{
        "fa19-cs425-g30-01.cs.illinois.edu",
        "fa19-cs425-g30-02.cs.illinois.edu",
        "fa19-cs425-g30-08.cs.illinois.edu",
        "fa19-cs425-g30-09.cs.illinois.edu",
    },
    "chakra": []string{"abc", "def"}}

//made for introducer
var ALL_HOSTS = map[string][]string{
   "all_hosts_info":[]string{
    "fa19-cs425-g30-01.cs.illinois.edu",
    "fa19-cs425-g30-02.cs.illinois.edu",
    "fa19-cs425-g30-03.cs.illinois.edu",
    "fa19-cs425-g30-04.cs.illinois.edu",
    "fa19-cs425-g30-05.cs.illinois.edu",
    "fa19-cs425-g30-06.cs.illinois.edu",
    "fa19-cs425-g30-07.cs.illinois.edu",
    "fa19-cs425-g30-08.cs.illinois.edu",
    "fa19-cs425-g30-09.cs.illinois.edu",
    "fa19-cs425-g30-10.cs.illinois.edu",
    }}

var HOST_TO_IP = map[string]string{
    "fa19-cs425-g30-01.cs.illinois.edu" : "172.22.156.98",
    "fa19-cs425-g30-02.cs.illinois.edu" : "172.22.152.103",
    "fa19-cs425-g30-03.cs.illinois.edu" : "172.22.154.99",
    "fa19-cs425-g30-04.cs.illinois.edu" : "172.22.156.99",
    "fa19-cs425-g30-05.cs.illinois.edu" : "172.22.152.104",
    "fa19-cs425-g30-06.cs.illinois.edu" : "172.22.154.100",
    "fa19-cs425-g30-07.cs.illinois.edu" : "172.22.156.100",
    "fa19-cs425-g30-08.cs.illinois.edu" : "172.22.152.105",
    "fa19-cs425-g30-09.cs.illinois.edu" : "172.22.154.101",
    "fa19-cs425-g30-10.cs.illinois.edu" : "172.22.156.101"}

//Introducer address
var INTRODUCER = "fa19-cs425-g30-01.cs.illinois.edu"

//default port to listen
var DEFAULT_PORT_TO_LISTEN = 34343
