package main

import (
  "fmt"
  "os"
  "bufio"
  "net"
  "encoding/json"
  "strconv"
  "time"
  "log"
//  "sort"
)

type InitialMessage struct{
  MessageType string
  Id []string
  Hostname []string
  IpAddress []string
  Timestamp []string
  Status []string
}

type Message struct {
  MessageType string
  Hostname string
  Timestamp string
}

type MembershipElement struct{
  id string
  hostname string
  ipAddress string
  timestamp string
  status string
}

type ServerInfo struct{
  membershipList map[string]*MembershipElement
  neighbourList []string
  id string
  hostname string
  port int
  heartbeatMap map[string]int32
}

func InitMembershipList(hostname string) map[string]*MembershipElement{
  var membershipList = make(map[string]*MembershipElement)
  for idx, hostname := range ALL_HOSTS["all_hosts_info"]{
    //TODO: hostname to IP mapping in config
    membershipList[hostname] = &MembershipElement{strconv.Itoa(idx+1), hostname, HOST_TO_IP[hostname], time.Now().Format("15:04:05.000000") ,"INIT"}
  }
  return membershipList
}

func InitMyServer(hostname string) *ServerInfo{
  var server ServerInfo

  server.membershipList = InitMembershipList(hostname)
  server.neighbourList =  MONITOR_NODE_MAPPING[hostname]
  server.hostname = hostname
  server.port = DEFAULT_PORT_TO_FAILURE_DETECTION_LISTEN

  server.heartbeatMap = make(map[string]int32)
  for _, neighbour := range server.neighbourList{
    server.heartbeatMap[neighbour] = 0
  }

  return &server
}


func commandListener(myServer *ServerInfo){
  addr := net.UDPAddr{
    IP: net.ParseIP(myServer.hostname),
    Port: myServer.port,
  }

  ser, err := net.ListenUDP("udp", &addr)
  if err != nil {
    log.Printf("Unable to listen for UDP connections. error:%s\n", err)
    return
  }

  for {
    resp := make([]byte,2048)
    n, err := ser.Read(resp)
    if err != nil {
      log.Println("Unable to read msg from socket. Error:%s\n", err)
      continue
    }
    go processMessage(myServer, resp, n)
  }
}

func join(myServer *ServerInfo){

  myServer.membershipList[myServer.hostname].status = "RUNNING"

  os.RemoveAll("/home/ss77/files/")
  pathErr := os.Mkdir("/home/ss77/files/", 0777)
  if pathErr != nil {
    fmt.Println(pathErr)
  }

  //SDFS
  liveServerBitMap[getIdFromHost(myServer.hostname)-1] = 1

  if !AmIIntroducer(myServer){
    conn, err := net.Dial("udp", INTRODUCER + ":" + "34343")
    if err != nil {
      log.Printf("Unable to contact INTRODUCER for joining. Error:%s\n", err)
      return
    }

    msg := Message{MessageType: "JOIN", Hostname:myServer.hostname,
                   Timestamp: time.Now().Format("15:04:05.000000")}
    jsonMsg    :=  getJSONfromMessage(msg)

    _, err = conn.Write(jsonMsg)
    if err != nil{
      log.Printf("Unable to send JOIN message to INTRODUCER. Error:%s\n", err)
    }
  }
}

func sendHeartbeats(myServer *ServerInfo){

  msg := Message{MessageType: "HEARTBEAT",Hostname:myServer.hostname,
	         Timestamp: ""}
  jsonMsg :=  getJSONfromMessage(msg)

  interval := time.Tick(1*time.Second)
  for range interval{
    for _, neigbour := range myServer.neighbourList{

      go func(neigbour string){
        conn, err := net.Dial("udp", neigbour + ":" + "34343")
         if err != nil {
            log.Printf("Unable to connect to host:%s to send HEARTBEAT. Error:%s\n", neigbour, err)
            return
        }

        _, err = conn.Write(jsonMsg)
        if err != nil{
          log.Println("Unable to send HEARTBEAT to host:%s. Error:%s\n", neigbour, err)
        }
      }(neigbour)
    }
  }
}

func monitorHeartbeats(myServer *ServerInfo){
  var failedHosts []string
  for {

    for host, timestamp := range myServer.heartbeatMap{
      currentTime := int32(time.Now().Unix())
      if (currentTime - timestamp) > 6{
        if myServer.membershipList[host].status == "RUNNING" && myServer.heartbeatMap[host] != 0{
           myServer.membershipList[host].status = "FAILED"
           myServer.heartbeatMap[host] = 0

           //SDFS
           liveServerBitMap[getIdFromHost(host)-1] = 0
           sendFsFailMessage("FAILED", host)

          //Maple-Juice
          //Non-Blocking message
          if myServer.hostname == INTRODUCER{
            select{
              case failChannel <- host:
                fmt.Println("Sent msg on failChannel for Maple Juice")
              default:
                fmt.Println("WARN: Not sending msg on failChannel for Maple Juice")
            }
          }


           failedHosts = append(failedHosts, host)
	   log.Printf("FAILURE detected! failedHost:%s currentHost:%s currentTime:%s lastheartbeatTimestamp:%s difference:%s\n",
                    host, myServer.hostname, currentTime, timestamp, (currentTime - timestamp))
        }
      }
    }

    for _, failedHost := range failedHosts{
      msg := Message{MessageType: "FAILURE", Hostname:failedHost, Timestamp: ""}

      jsonMsg :=  getJSONfromMessage(msg)
      for _, neighbour := range myServer.neighbourList{
        conn, err := net.Dial("udp", neighbour + ":" + "34343")
        if err != nil {
          log.Printf("Unable to connect to host:%s to send FAILURE msg. Error:%s\n", 
                      neighbour, err)
          continue
        }

        _, err = conn.Write(jsonMsg)
        if err != nil{
          log.Println("Unable to send FAILURE msg to host:%s. Error:%s\n",
                       neighbour, err)
        }
      }
    }

    failedHosts = nil
    time.Sleep(1*time.Second)

  }
}

func leave(myServer *ServerInfo){
  if myServer.membershipList[myServer.hostname].status != "RUNNING"{
    log.Println("Not leaving as it is not running :-)")
    return
  }

  msg := Message{MessageType: "LEAVE", Hostname:myServer.hostname,
                 Timestamp: "466464646"}
  jsonMsg := getJSONfromMessage(msg)

  for _,neighbour := range ALL_HOSTS["all_hosts_info"]{
    conn, err := net.Dial("udp", neighbour + ":" + "34343")
    if err != nil {
      log.Printf("Unable to connect to host:%s to send LEAVE message. Error:%s\n", 
                  neighbour, err)
      continue
    }
    _, err = conn.Write(jsonMsg)
    if err != nil{
      log.Printf("Unable to send leave message to neighbour. Error:%s\n", err)
    }
  }

  myServer.membershipList[myServer.hostname].status = "LEFT"
  liveServerBitMap[getIdFromHost(myServer.hostname)-1] = 0


  log.Println(myServer.hostname + " left peacefully...")
}

func processMessage(myServer *ServerInfo, resp []byte, n int){

  respJSON := []byte(string(resp[:n]))
  respMessage := getMessagefromJSON(respJSON)

  log.Printf("Received Message - MessageType:%s Hostname:%s\n", respMessage.MessageType,
                                                                 respMessage.Hostname)

  if respMessage.MessageType == "INITIALISE"{
     initialRespMessage := getInitialMessagefromJSON(respJSON)
     for idx, status := range initialRespMessage.Status{
       myServer.membershipList[initialRespMessage.Hostname[idx]].status = status
       myServer.membershipList[initialRespMessage.Hostname[idx]].timestamp = initialRespMessage.Timestamp[idx]

       if status == "RUNNING"{
        liveServerBitMap[getIdFromHost(initialRespMessage.Hostname[idx])-1]= 1
       }
     }

  }else if respMessage.MessageType == "JOIN"{
    myServer.membershipList[respMessage.Hostname].status = "RUNNING"
    myServer.membershipList[respMessage.Hostname].timestamp = respMessage.Timestamp

    //SDFS
    liveServerBitMap[getIdFromHost(respMessage.Hostname)-1] = 1

    /*
    os.RemoveAll("/home/ss77/deltest/")
    pathErr := os.Mkdir("/home/ss77/deltest/", 0777)

    if pathErr != nil {
      fmt.Println(pathErr)
    }*/

    if AmIIntroducer(myServer){
      msg := Message{MessageType: "JOIN", Hostname:respMessage.Hostname,
                     Timestamp: respMessage.Timestamp}

      jsonMsg :=  getJSONfromMessage(msg)
      for _, hostname := range ALL_HOSTS["all_hosts_info"]{
        if hostname != respMessage.Hostname &&  hostname != (myServer.hostname){
          conn, err := net.Dial("udp", hostname + ":" + "34343")
          if err != nil {
            log.Printf("Unable to connect to host:%s to send JOIN message from INRODUCER. Error:%s\n", 
                        hostname, err)
            continue
          }

          _, err = conn.Write(jsonMsg)
          if err != nil{
            log.Println("Unable to send JOIN message to host:%s from INTRODUCER. Error:%s\n",
                         hostname, err)
          }
        }
      }

      //Sending membership list to new joinee
      var initialMessage InitialMessage
      initialMessage.MessageType = "INITIALISE"
      for _, v := range myServer.membershipList{
          member := *v
          initialMessage.Id = append(initialMessage.Id, member.id)
          initialMessage.Hostname = append(initialMessage.Hostname, member.hostname)
          initialMessage.IpAddress = append(initialMessage.IpAddress, member.ipAddress)
          initialMessage.Timestamp = append(initialMessage.Timestamp, member.timestamp)
          initialMessage.Status = append(initialMessage.Status, member.status)
      }

      jsonMsg =  getJSONfromInitialMessage(initialMessage)
      conn, err := net.Dial("udp", respMessage.Hostname + ":" + "34343")
      if err != nil {
        log.Printf("Unable to connect to host:%s to send INITIALISE message from INRODUCER. Error:%s\n",
                    respMessage.Hostname, err)
      }

      _, err = conn.Write(jsonMsg)
      if err != nil{
        log.Println("Unable to send INITIALISE message to host:%s from INTRODUCER. Error:%s\n", 
                     respMessage.Hostname, err)
      }
    }
    log.Println("Successfully JOINED node:%s in memebership list", respMessage.Hostname)

  }else if respMessage.MessageType == "LEAVE"{
     myServer.membershipList[respMessage.Hostname].status = "LEFT"

     //SDFS
     liveServerBitMap[getIdFromHost(respMessage.Hostname)-1] = 0

     log.Println("Successfully REMOVED node:%s in memebership list as it LEFT", respMessage.Hostname)

  }else if respMessage.MessageType == "FAILURE" && respMessage.Hostname != myServer.hostname {
    if myServer.membershipList[respMessage.Hostname].status == "RUNNING"{
      myServer.membershipList[respMessage.Hostname].status = "FAILED"

      //SDFS
      liveServerBitMap[getIdFromHost(respMessage.Hostname)-1] = 0
      sendFsFailMessage("FAILED", respMessage.Hostname)

      //Maple-Juice
      //Non-Blocking message
      if myServer.hostname == INTRODUCER{
        select{
          case failChannel <- respMessage.Hostname:
            fmt.Println("Sent msg on failChannel for Maple Juice")
          default:
            fmt.Println("WARN: Not sending msg on failChannel for Maple Juice")
        }
      }

      msg := Message{MessageType: "FAILURE", Hostname:respMessage.Hostname,
                     Timestamp: respMessage.Timestamp}

      jsonMsg :=  getJSONfromMessage(msg)
      for _, neighbour := range myServer.neighbourList{
        conn, err := net.Dial("udp", neighbour + ":" + "34343")
        if err != nil {
          log.Printf("Unable to connect to NEIGHBOUR:%s to send FAILURE message. Error:%s\n", 
                      neighbour, err)
          continue
        }

        _, err = conn.Write(jsonMsg)
        if err != nil{
          log.Println("Unable to send FAILURE message to NEIGHBOUR:%s. Error:%s\n", 
                       neighbour, err)
        }
      }

      log.Println("Successfully REMOVED node:%s in memebership list as it FAILED", respMessage.Hostname)
    }

  }else if respMessage.MessageType == "HEARTBEAT"{
     myServer.heartbeatMap[respMessage.Hostname] = int32(time.Now().Unix())
     log.Println("Succesfully Received HEARTBEAT from host:%s", respMessage.Hostname)
  }
}

func FailureDetector(){

 //Opening log file
 logFile, err := os.OpenFile("mp2.log", os.O_RDWR | os.O_CREATE , 0666)
 if err != nil{
   log.Fatalln("Unable to open log file! Exiting...")
 }
 defer logFile.Close()
 log.SetOutput(logFile)

  hostname, err := os.Hostname()
  if err != nil{
    log.Fatalln("Unable to get hostname! Exiting...")
  }
  myServer := InitMyServer(hostname)

  go commandListener(myServer)

  //Take the user input and process the command
  //Running in infinite loop
  for {
    fmt.Printf(`Please give the input. Follwing are the options:
        1. Print membership list
        2. Print my ID
        3. Join the system         
        4. Leave the system
                 
        Press 1-4 for the required functionality:`)
    fmt.Println()

    commandReader := bufio.NewReader(os.Stdin)
    command, _, readError := commandReader.ReadRune()
    if readError != nil{
      log.Printf("Unable to read user input. Error:%s\n", readError)
    }

    switch command{
      case '1':
        //members := make([]MembershipElement, 0, len(myServer.membershipList))
        for _, memberElement := range myServer.membershipList {
          fmt.Printf("%+v\n", *memberElement)
          //members = append(members, *memberElement)
        }

        //fmt.Println(sort.Sort(members))
      case '2':

        fmt.Printf("Id is IP_address: %s Timestamp: %s\n", myServer.membershipList[myServer.hostname].ipAddress,
                                                           myServer.membershipList[myServer.hostname].timestamp)
      case '3':

        join(myServer)
        go sendHeartbeats(myServer)
        go monitorHeartbeats(myServer)

      case '4':
        leave(myServer)
    }
  }
}

func AmIIntroducer(myServer *ServerInfo) bool{
  return myServer.hostname == INTRODUCER
}

func getMessagefromJSON(jsonMessage []byte) Message {
  var message Message
  err := json.Unmarshal(jsonMessage, &message)
  if err != nil {
    log.Printf("Unable to unmarshal message. Error:%s\n", err)
  }
  return message
}

func getJSONfromMessage(message Message) []byte {
  var jsonMessage []byte
  jsonMessage, err := json.Marshal(message)
  if err != nil {
    log.Printf("Unable to marshal message. Error:%s\n", err)
  }
  return jsonMessage
}

func getInitialMessagefromJSON(jsonMessage []byte) InitialMessage {
  var message InitialMessage
  err := json.Unmarshal(jsonMessage, &message)
  if err != nil {
    log.Printf("Unable to unmarshal initial message. Error:%s\n", err)
  }
  return message
}

func getJSONfromInitialMessage(message InitialMessage) []byte {
  var jsonMessage []byte
  jsonMessage, err := json.Marshal(message)
  if err != nil {
    log.Printf("Unable to marshal initial message. Error:%s\n", err)
  }
  return jsonMessage
}
