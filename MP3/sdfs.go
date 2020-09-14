package main

import (
  "fmt"
  "log"
  "os"
  "os/exec"
  "bufio"
  "strings"
  "net"
  "encoding/json"
  "strconv"
  "path/filepath"
  "time"
  "math/rand"
)

//map to keep which servers are alive
var liveServerBitMap = make([]int, 10)

//FileSystem Server Type
type FileSystemServer struct{
  hostname string
  port int
  FileTable map[string][]int
  PutProgressFiles map[string]int
}

//FsMessage Type
type FileSystemMessage struct{
  ID string //messageType string
  Data string //fromHost string
  Num string //filename string
  Deg []int //replicaIds []string
}

//Initialisig FileSytem Server
func InitMyFileServer(hostname string) *FileSystemServer{
  var myFsServer FileSystemServer

  myFsServer.hostname = hostname
  myFsServer.port = DEFAULT_PORT_TO_SDFS_LISTEN
  myFsServer.FileTable = make(map[string][]int)
  myFsServer.PutProgressFiles = make(map[string]int)

  return &myFsServer
}

//Function which listens for various FsMessages
func fsCommandListener(myFsServer *FileSystemServer){
  addr := net.UDPAddr{
    IP: net.ParseIP(myFsServer.hostname),
    Port: myFsServer.port,
  }

  ser, err := net.ListenUDP("udp", &addr)
  if err != nil {
    log.Printf("Unable to listen for UDP connections. error:%s\n", err)
    return
  }

  for {
    msg := make([]byte,2048)
    n, err := ser.Read(msg)
    if err != nil {
      log.Println("Unable to read msg from socket. Error:%s\n", err)
      continue
    }
    go processFsMessage(myFsServer, msg, n)
  }

}

//Function to process Fs Messages
func processFsMessage(myFsServer *FileSystemServer, msg []byte, n int){
  msgJSON := []byte(string(msg[:n]))
  myFsMsg := getFsMessagefromJSON(msgJSON)

  log.Printf("Received Message - MessageType:%s Hostname:%s\n", myFsMsg.ID,
                                                                myFsMsg.Data)
  fmt.Printf("Received Message - MessageType:%s Hostname:%s\n", myFsMsg.ID,
                                                                myFsMsg.Data)
  
  //Msg sent before PUT msg
  if myFsMsg.ID == "PUT_START"{
   fmt.Println("received PUT_START")
   myFsServer.PutProgressFiles[myFsMsg.Num] = 1
   
  //Put msg
  }else if myFsMsg.ID == "PUT"{
    fmt.Println("received PUT")
    var serverList = make([]int, 10)
    for _,id := range myFsMsg.Deg{
      fmt.Println(id)
      serverList[id-1] = 1
    }
    myFsServer.FileTable[myFsMsg.Num] = serverList
  
  //Msg sent after PUT msg
  }else if myFsMsg.ID == "PUT_END"{
    fmt.Println("received PUT_END")
    delete(myFsServer.PutProgressFiles, myFsMsg.Num)
  
  //DELETE Msg
  }else if myFsMsg.ID == "DELETE"{
    fmt.Println("received DELETE")
    delete(myFsServer.FileTable, myFsMsg.Num)
  
  //Msg received from localhost when failure is detected
  //re-replication logic here too
  }else if myFsMsg.ID == "FAILED"{
    possibleFilesToReplicate := make(map[string][]int)

    failedHostIdx := getIdFromHost(myFsMsg.Data) - 1
    myHostId := getIdFromHost(myFsServer.hostname)

    //Choosing highest replica node id
    for sdfsfilename,_ := range myFsServer.FileTable{
      if myFsServer.FileTable[sdfsfilename][failedHostIdx] == 1{
        myFsServer.FileTable[sdfsfilename][failedHostIdx] = 0
        fsReplicaIds := getAllReplicas(myFsServer, sdfsfilename)

        fmt.Printf("filename: ")
        fmt.Println(sdfsfilename)

        fmt.Printf("fsReplicaIds: ")
        fmt.Println(fsReplicaIds)

        if fsReplicaIds[len(fsReplicaIds) - 1] == myHostId{
          possibleFilesToReplicate[sdfsfilename] = fsReplicaIds
        }
      }
    }

    //re-replicating
    for filename, fsReplicaIds := range possibleFilesToReplicate{
      liveNodes := getLiveNodes()
      possibleNodestoReplicate := getRemainingNodes(liveNodes, fsReplicaIds)

      //TODO: randomly select a node. then scp the file and then send a PUT message
      rand.Seed(time.Now().UnixNano())
      nodeToReplicateIdx := rand.Intn(len(possibleNodestoReplicate))

      fmt.Println(possibleNodestoReplicate)
      fmt.Printf("re-replicate node idx: %d\n", nodeToReplicateIdx)
      fmt.Printf("re-replicate node: %s\n", getHostFromId(possibleNodestoReplicate[nodeToReplicateIdx]))

      sdfsFilePath := getSDFSSubDir() + filename
      sdfsFileDir := filepath.Dir(sdfsFilePath)

      CreateRemoteDirIfNotExist(sdfsFileDir, getHostFromId(possibleNodestoReplicate[nodeToReplicateIdx]))

      cmd := exec.Command("scp", getSDFSSubDir() + filename,
                          getHostFromId(possibleNodestoReplicate[nodeToReplicateIdx]) +
                          ":" + getSDFSSubDir() + filename)
      fmt.Println(cmd)
      err := cmd.Run()
      fmt.Println(err)

      //Updating my FileTable
      myFsServer.FileTable[filename][possibleNodestoReplicate[nodeToReplicateIdx]-1] = 1

      fmt.Printf("Old Replica Ids: ")
      fmt.Println(fsReplicaIds)

      //Sending PUT mesg to update filetable of others
      newFsReplicaIds := append(fsReplicaIds, possibleNodestoReplicate[nodeToReplicateIdx])

      fmt.Printf("New Replica Ids: ")
      fmt.Println(newFsReplicaIds)
      
      sendFsMessage(myFsServer, "PUT", filename, newFsReplicaIds)
    }
  }else if myFsMsg.ID == "CREATE"{                                                                                          
      CreateDirIfNotExist(myFsMsg.Num)
  }

}

//function send FsMessage
func sendFsMessage(myFsServer *FileSystemServer, msgType string, sdfsfilename string, replicaIds []int){
  var fsMsg FileSystemMessage

  fmt.Println("ReplicaIds:")
  fmt.Println(replicaIds)
  fsMsg.ID = msgType
  fsMsg.Data = myFsServer.hostname
  fsMsg.Deg = replicaIds
  fsMsg.Num = sdfsfilename

  jsonFsMsg :=  getJSONfromFsMessage(fsMsg)
  for _, server := range ALL_HOSTS["all_hosts_info"]{
    if server != myFsServer.hostname{
      conn, err := net.Dial("udp", server + ":" + "34344")
      if err != nil {
        log.Printf("Unable to connect to SERVER:%s to send PUT message. Error:%s\n", 
                    server, err)
        continue
      }

      _, err = conn.Write(jsonFsMsg)
      if err != nil{
        log.Println("Unable to send PUT message to SERVER:%s. Error:%s\n", 
                     server, err)
      }
    }
  }
}

//Separate func to send FAIL msg as it has to be sent to only localhost
func sendFsFailMessage(msgType string, hostname string){
  var fsMsg FileSystemMessage
  var replicaIds []int
  var filename string

  fmt.Println("ReplicaIds:")
  fsMsg.ID = msgType
  fsMsg.Data = hostname
  fsMsg.Deg = replicaIds // dummy
  fsMsg.Num = filename //dummy

  jsonFsMsg :=  getJSONfromFsMessage(fsMsg)
  conn, err := net.Dial("udp", "127.0.0.1" + ":" + "34344")
  if err != nil {
    log.Printf("Unable to connect to SERVER:%s to send PUT message. Error:%s\n",
                "127.0.0.1", err)
    return
  }

  _, err = conn.Write(jsonFsMsg)
  if err != nil{
    log.Println("Unable to send PUT message to SERVER:%s. Error:%s\n",
                 "127.0.0.1", err)
  }
}

//Send CREATE message
func sendFsDirCreateMessage(msgType string, hostname string, filename string){
  var fsMsg FileSystemMessage
  var replicaIds []int

  fmt.Println("ReplicaIds:")
  fsMsg.ID = msgType
  fsMsg.Data = hostname
  fsMsg.Deg = replicaIds // dummy
  fsMsg.Num = filename

  jsonFsMsg :=  getJSONfromFsMessage(fsMsg)
  conn, err := net.Dial("udp", hostname + ":" + "34344")
  if err != nil {
    log.Printf("Unable to connect to SERVER:%s to send CREATE message. Error:%s\n",
                hostname, err)
    return
  }

  _, err = conn.Write(jsonFsMsg)
  if err != nil{
    log.Println("Unable to send CREATE message to SERVER:%s. Error:%s\n",
                 hostname, err)
  }
}


//Initialize function
func InitializeFailureDetectorandFileSystem(){

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
  myFsServer := InitMyFileServer(hostname)
  go fsCommandListener(myFsServer)
  go commandListener(myServer)



  //Take the user input and process the command
  //Running in infinite loop
  for {
    fmt.Printf(`Please give the input. Follwing are the options:
        1. Print membership list
        2. Print my ID
        3. Join the system
        4. Leave the system
        5. Put local file in the filesystem
        6. Get file from filesystem
        7. Delete file from filesystem
        8. ls command - where all given file is present
        9. store command - which files are present in this node
        a. Print fileTable
        b. Print liveNodes

        Press any of the above chars for the required functionality:`)
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
      case '5':
        fmt.Printf("Enter the command as : put <localfilename> <sdfsfilename>")
        command, readError := commandReader.ReadString('\n')
        command, readError = commandReader.ReadString('\n')
        if readError != nil{
          log.Printf("Unable to read user input. Error:%s\n", readError)
        }

        command = strings.TrimSpace(command)
        command_args  := strings.Split(command, " ")
        if len(command_args) != 3{
          fmt.Printf("Format should be : put <localfilename> <sdfsfilename>")
        }else{
          put(myFsServer, command_args[1], command_args[2])
        }
      case '6':
        fmt.Printf("Enter the command as : get <sdfsfilename> <localfilename>")
        command, readError := commandReader.ReadString('\n')
        command, readError = commandReader.ReadString('\n')
        if readError != nil{
          log.Printf("Unable to read user input. Error:%s\n", readError)
        }

        command = strings.TrimSpace(command)
        command_args  := strings.Split(command, " ")
        if len(command_args) != 3{
          fmt.Printf("Format should be : get <sdfsfilename> <localfilename>")
        }else{
          //Create localfilepath if not present
          localfilePath := command_args[2]
          localdir := filepath.Dir(localfilePath)
          os.MkdirAll(localdir, os.ModePerm)

          get(myFsServer, command_args[1], command_args[2])
        }
      case '7':
        fmt.Printf("Enter the command as : delete <sdfsfilename>")
        command, readError := commandReader.ReadString('\n')
        command, readError = commandReader.ReadString('\n')
        if readError != nil{
          log.Printf("Unable to read user input. Error:%s\n", readError)
        }

        command = strings.TrimSpace(command)
        command_args  := strings.Split(command, " ")
        if len(command_args) != 2{
          fmt.Printf("Format should be : delete <sdfsfilename>")
        }else{
          remove(myFsServer, command_args[1])
        }
      case '8':
        fmt.Printf("Enter the command as : ls <sdfsfilename>")
        command, readError := commandReader.ReadString('\n')
        command, readError = commandReader.ReadString('\n')
        if readError != nil{
          log.Printf("Unable to read user input. Error:%s\n", readError)
        }

        command = strings.TrimSpace(command)
        command_args  := strings.Split(command, " ")
        if len(command_args) != 2{
          fmt.Printf("Format should be : ls <sdfsfilename>")
        }else{
          ls(myFsServer, command_args[1])
        }
      case '9':
        fmt.Println("Inside option 9")
        store()
      case 'a':
        for filename, replicas := range myFsServer.FileTable {
          fmt.Println("Global File Table:")
          fmt.Println(filename)
          fmt.Println(replicas)
        }
      case 'b':
        for idx, live := range liveServerBitMap{
          fmt.Printf("%s: %d\n", getHostFromId(idx+1), live)
        }
    }
  }
}

// PUT command
func put(myFsServer *FileSystemServer, localfilename string, sdfsfilename string){

  hostname, err := os.Hostname()
  if err != nil{
    log.Fatalln("Unable to get hostname! Exiting...")
  }

  if _, present := myFsServer.PutProgressFiles[sdfsfilename]; present{
      showWarningToUser(sdfsfilename)
      return
  }

  var replicaIdsBitMap = make([]int, 10)
  var replicaIds []int

  //SEND PUT_START message before doing put
  sendFsMessage(myFsServer, "PUT_START", sdfsfilename, replicaIds)

  for _, neighbour := range MONITOR_NODE_MAPPING[hostname]{
    if liveServerBitMap[getIdFromHost(neighbour) - 1] == 0{
      continue
    }

    sdfsFilePath := getSDFSSubDir() + sdfsfilename
    sdfsFileDir := filepath.Dir(sdfsFilePath)

    fmt.Printf("sdfsDir: ")
    fmt.Println(sdfsFileDir)

    CreateRemoteDirIfNotExist(sdfsFileDir, neighbour) 
    cmd := exec.Command("scp", localfilename,
                        neighbour + ":" + getSDFSSubDir() + sdfsfilename)
    fmt.Println(cmd)
    err := cmd.Run()
    fmt.Println(err)
    //TODO: Understand what err is
    replicaIdsBitMap[getIdFromHost(neighbour) - 1] = 1
    replicaIds = append(replicaIds, getIdFromHost(neighbour))
    fmt.Println("print replicaIds...")
    fmt.Println(replicaIds)
  }

  sendFsMessage(myFsServer, "PUT", sdfsfilename, replicaIds)
  myFsServer.FileTable[sdfsfilename] = replicaIdsBitMap

  //Completing PUT transaction
  sendFsMessage(myFsServer, "PUT_END", sdfsfilename, replicaIds)
}

// GET command
func get(myFsServer *FileSystemServer, sdfsfilename string, localfilename string){

  filePath := getSDFSSubDir() + sdfsfilename
  fmt.Println("get filePath:" + filePath)
  fmt.Println(sdfsfilename)
  if replicaIds, present := myFsServer.FileTable[sdfsfilename]; present{
    fmt.Println(sdfsfilename + ":")
    for idx, replicaId := range replicaIds{
      if replicaId == 1{
        hostname := getHostFromId(idx+1)
        fmt.Println("hostname:" + hostname)
        cmd := exec.Command("scp", hostname + ":" + filePath, localfilename)
        fmt.Println(cmd)
        err := cmd.Run()
        fmt.Println(err)
        break;
      }
    }
  }else{
    fmt.Println("Sorry! File not Present :-(")
  }
}

//Remove Command
func remove(myFsServer *FileSystemServer, sdfsfilename string){
  _, present := myFsServer.FileTable[sdfsfilename]
  if !present{
    fmt.Println("Sorry! Can't delete a file which is not there :-(")
    return
  }

  filePath := getSDFSSubDir() + "/" + sdfsfilename
  fmt.Println("delete filePath:" + filePath)
  for idx, replicaId := range myFsServer.FileTable[sdfsfilename]{
    fmt.Println(idx, replicaId)
    if replicaId != 0{
      hostname := getHostFromId(idx+1)
      fmt.Println("hostname:" + hostname)
      cmd := exec.Command("ssh", hostname, "rm", filePath)
      fmt.Println("delete command: ")
      fmt.Println(cmd)
      err := cmd.Run()
      fmt.Println(err)
    }
  }
  delete(myFsServer.FileTable, sdfsfilename)

  var replicaIds []int
  sendFsMessage(myFsServer, "DELETE", sdfsfilename, replicaIds)
}

// LS command
func ls(myFsServer *FileSystemServer, sdfsfilename string){
  if replicaIds, present := myFsServer.FileTable[sdfsfilename]; present{
    fmt.Println(sdfsfilename + ":")
    for idx, replicaId := range replicaIds{
      if replicaId == 1{
        fmt.Println(getHostFromId(idx+1))
      }
    }
  }else{
    fmt.Println("Sorry! File not Present :-(")
  }
}

// STORE command
func store(){
  //cmd := exec.Command("ls -lrt", getSDFSSubDir())
  var dirToRun = "/home/ss77/files"
  var err = os.Chdir(dirToRun)
  
  if err != nil {
     panic(err)
  }
  var cmdName = "ls"

  var cmd = exec.Command(cmdName, "-a", "-l")
  output, err := cmd.Output()
  if err != nil {
    fmt.Println(err)
  }
  fmt.Printf("%v\n", string(output))
}

func main(){
  InitializeFailureDetectorandFileSystem()
}

// Helper func to get SDFS root dir
func getSDFSSubDir() string{
  var slash = "/"
  var path = SDFS_ROOT + slash + SDFS_SUBDIR + slash

  fmt.Println(path)

  return path
}

// Fs Message Marshalling & UnMarshalling funcs

func getFsMessagefromJSON(jsonMessage []byte) FileSystemMessage {
  var message FileSystemMessage
  err := json.Unmarshal(jsonMessage, &message)
  if err != nil {
    log.Printf("Unable to unmarshal message. Error:%s\n", err)
  }
  return message
}

func getJSONfromFsMessage(message FileSystemMessage) []byte {
  var jsonMessage []byte
  jsonMessage, err := json.Marshal(message)
  if err != nil {
    log.Printf("Unable to marshal message. Error:%s\n", err)
  }
  return jsonMessage
}

// Helper funcs to convert b/w Hostname and its ID

func getIdFromHost(hostname string) int{
  temp := strings.Split(hostname, ".")[0]
  temp2 := strings.Split(temp,"-")[3]

  id,_ := strconv.Atoi(temp2)
  fmt.Println("hostidstring : " + temp2)
  return id
}

func getHostFromId(id int) string{
  var hostIdStr = lpad(strconv.Itoa(id),"0",2)
  return "fa19-cs425-g30-" + hostIdStr + ".cs.illinois.edu"
}

func lpad(s string,pad string, plength int) string{
  for i:=len(s);i<plength;i++{
      s = pad + s
  }
  return s
}

//Handling Simulatneous PUT
func showWarningToUser(filename string) {

  userInput := make(chan rune, 1)

  go takeUserInput(userInput)

  select{
    case res := <-userInput:
      fmt.Printf("Request received is %c", res)
      fmt.Println("Ignoring PUT request...")
      break
    case <-time.After(30*time.Second):
      fmt.Println("Request Timed out. Ignoring PUT command..")
      break
  }

  return
}

//Handling Simultaneous PUT
func takeUserInput(userInput chan rune){
  fmt.Println("Another process is doing \"PUT\" operation on same file....")
  fmt.Println("Do you want to continue [Y/N]?")

  commandReader := bufio.NewReader(os.Stdin)
  command, _, readError := commandReader.ReadRune()
      if readError != nil{
      log.Printf("Unable to read user input. Error:%s\n", readError)
    }else{
      if command == 'N' || command == 'n'{
        userInput <- command
      }
    }
}

//Get All Replicas
func getAllReplicas(myFsServer *FileSystemServer, sdfsfilename string) []int{
  var replicaIds []int

  for idx,present := range myFsServer.FileTable[sdfsfilename]{
    if present == 1{
      replicaIds = append(replicaIds, idx+1)
    }
  }

  return replicaIds
}

//Get LiveNodes
func getLiveNodes() map[int]int{
  liveNodes := make(map[int]int)

  fmt.Printf("liveserevrbitmap: ")
  fmt.Println(liveServerBitMap)
  for idx, present := range liveServerBitMap{
    if present == 1{
      liveNodes[idx + 1] = 1
    }
    fmt.Printf("LiveNodes: %+v\n", liveNodes)
  }

  fmt.Printf("LiveNodes: %+v\n", liveNodes)
  return liveNodes
}

//Get LiveNodes - NodesWhichHasReplica
func getRemainingNodes(liveNodes map[int]int, fsReplicaIds []int) []int{

  var remainingNodes []int
  for _,replicaId := range fsReplicaIds{
    delete(liveNodes, replicaId)
  }

  for node,_ := range liveNodes{
    remainingNodes = append(remainingNodes, node)
  }

  fmt.Printf("LiveNodes: %+v\n", liveNodes)
  fmt.Println(fsReplicaIds)
  fmt.Printf("RemainingNodes: ")
  fmt.Println(remainingNodes)

  return remainingNodes
}

//Create Directories if not exist
func CreateDirIfNotExist(dir string) {
    if _, err := os.Stat(dir); os.IsNotExist(err) {
        err = os.MkdirAll(dir, 0755)
        if err != nil {
            fmt.Println("NOTHING HAPPENED")
        }
    }   
}

//Create Remote Directories
func CreateRemoteDirIfNotExist(dir string, host string){
  sendFsDirCreateMessage("CREATE", host, dir)
} 
