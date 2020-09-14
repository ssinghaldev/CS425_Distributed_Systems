package main

import(
  "fmt"
  "log"
  "net"
  "time"
)

//mapleJuice command type
type MapleJuiceCommand struct{
  commandType string
  commandExec string
  numWorkers int
  sdfsSrcDir string
  sdfsIntermediateFilenamePrefix string
  sdfsDestFilename string
  deleteInput int
  partitionType int
}

//mapleJuice message - b/w master and worker
type MapleJuiceMessage struct{
  Id string //messageType string
  Data string //executableCommand string
  Deg []string //inputFiles []string
}

//maplejuice message - b/w worker and master
type AckMessage struct{
  command string
  host string
  filename string
}

var commandQueue = make(chan MapleJuiceCommand)
var ackChannel   = make(chan AckMessage)
var failChannel  = make(chan string)

//mjCommandListener
func mjCommandListener(hostname string, port int){
  addr := net.UDPAddr{
    IP: net.ParseIP(hostname),
    Port: port,
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
    go processMJMessage(msg, n)
  }
}

//processMJMessage
func processMJMessage(msg []byte, n int){
  msgJSON := []byte(string(msg[:n]))
  mjMsg := getMJMessagefromJSON(msgJSON)

  log.Printf("Received Message - MessageType:%s Command:%s\n", mjMsg.Id,
                                                               mjMsg.Data)
  fmt.Printf("Received Message - MessageType:%s Command:%s\n", mjMsg.Id,
                                                                mjMsg.Data)
  fmt.Println(mjMsg.Deg)
  if mjMsg.Id == "maple"{
    processMapleMessage(mjMsg.Data, mjMsg.Deg)
  }else if mjMsg.Id == "juice"{
    processJuiceMessage(mjMsg.Data, mjMsg.Deg)
  }else if mjMsg.Id == "maple_ack"{
    processAckMessage("maple", mjMsg.Data, mjMsg.Deg)
  }else if mjMsg.Id == "juice_ack"{
    processAckMessage("juice", mjMsg.Data, mjMsg.Deg)
  }
}

func processMapleMessage(execCommand string, inputFiles []string){
  //Check whether execCommand is present locally, if not get it
  execCommandPath := getSDFSSubDir() + "/" + execCommand
  if !isSDFSFilePresentLocally(execCommand){
    execCommandPath = getFileLocally(execCommand)
  }
  fmt.Println("execCommandPath:%s\n" , execCommandPath)

  outputFilePath := getTmpFilePath()
  fmt.Println("outputFilePath:%s", outputFilePath)

  //Check whether input file is present locally
  //If it is, then directly give the path to execCommand
  //If not, execute "get" command to get it locally, then give the path
  for _, inputFile := range inputFiles{
    inputFilePath := getSDFSSubDir() + "/" + inputFile
    if !isSDFSFilePresentLocally(inputFile){
      inputFilePath = getFileLocally(inputFile)
    }
    fmt.Println("inputFilePath:%s\n" , inputFilePath)
    executeProcess(execCommandPath, inputFilePath, outputFilePath)
  }

  copyOutputFileToMaster(outputFilePath)

  //send "ack" to master
  var oneFileList []string
  oneFileList = append(oneFileList, outputFilePath)
  sendMJMessage("maple_ack", INTRODUCER , myFsServer.hostname, oneFileList)
}

func processJuiceMessage(execCommand string, inputFiles []string){
  //Check whether execCommand is present locally, if not get it
  execCommandPath := getSDFSSubDir() + "/" + execCommand
  if !isSDFSFilePresentLocally(execCommand){
    execCommandPath = getFileLocally(execCommand)
  }
  fmt.Println("execCommandPath:%s\n" , execCommandPath)

  outputFilePath := getTmpFilePath()
  fmt.Println("outputFilePath:%s", outputFilePath)

  //Check whether input file is present locally
  //If it is, then directly give the path to execCommand
  //If not, execute "get" command to get it locally, then give the path
  for _, inputFile := range inputFiles{
    inputFilePath := getSDFSSubDir() + "/" + inputFile
    if !isSDFSFilePresentLocally(inputFile){
      inputFilePath = getFileLocally(inputFile)
    }
    fmt.Println("inputFilePath:%s\n" , inputFilePath)
    executeProcess(execCommandPath, inputFilePath, outputFilePath)
  }

  copyOutputFileToMaster(outputFilePath)

  //send "ack" to master
  var oneFileList []string
  oneFileList = append(oneFileList, outputFilePath)
  sendMJMessage("juice_ack", INTRODUCER , myFsServer.hostname, oneFileList)
}

//Below portion of the code will be executed by Master only
func processMapleJuiceCommand(){
  for{
    cmd := <-commandQueue
    fmt.Printf("Given Command:%+v\n", cmd)
    if cmd.commandType == "maple"{
      processMapleCommand(cmd)
    }else if cmd.commandType == "juice" {
      processJuiceCommand(cmd)
    }
  }
}

//Master processing Maple command
func processMapleCommand(cmd MapleJuiceCommand){
  startTime := time.Now()

  fmt.Println("Maple Command started")
  files := readFilesFromSDFSDir(cmd.sdfsSrcDir)
  if len(files) == 0{
    log.Printf("Exiting Maple command as unable to get input files error")
    return
  }

  partitionFiles := hashPartition(cmd.numWorkers, files)
  for hostname, fileList := range partitionFiles{
    go sendMJMessage(cmd.commandType, hostname, cmd.commandExec, fileList)
  }

  processedFileList := monitorWorkerProgress(cmd, partitionFiles)
  processWorkerMapleFiles(processedFileList, cmd.sdfsIntermediateFilenamePrefix)

  elapsedTime := time.Since(startTime)
  fmt.Println("Map job time taken:", elapsedTime)

  fmt.Println("Maple Command completed")
}

//Master processing Juice command
func processJuiceCommand(cmd MapleJuiceCommand){
  startTime := time.Now()

  fmt.Println("Juice Started")
  files := readFilesFromSDFSDir(cmd.sdfsSrcDir)
  if len(files) == 0{
    log.Printf("Exiting Maple command as unable to get input files error")
    return
  }

  var partitionFiles map[string][]string
  if cmd.partitionType == 0{
    partitionFiles = hashPartition(cmd.numWorkers, files)
  }else{
    partitionFiles = rangePartition(cmd.numWorkers, files)
  }

  for hostname, fileList := range partitionFiles{
    go sendMJMessage(cmd.commandType, hostname, cmd.commandExec, fileList)
  }

  processedFileList := monitorWorkerProgress(cmd, partitionFiles)
  processWorkerJuiceFiles(processedFileList, cmd.sdfsDestFilename)

  if cmd.deleteInput == 1{
    intermediateFilesFolder := LOCAL_FILE_ROOT + "/"  + cmd.sdfsSrcDir
    deleteFolder(intermediateFilesFolder)
    deleteSDFSDir(cmd.sdfsSrcDir)
  }

  elapsedTime := time.Since(startTime)
  fmt.Println("Reduce job time taken:", elapsedTime)

  fmt.Println("Juice completed")
}

//master processing ACK message
func processAckMessage(command string, fromHost string, outputFileList []string){
  fmt.Println("Received ACK Message")
  fmt.Printf("command:%s fromHost:%s Outputfilepath:%s\n", command, fromHost, outputFileList[0])

  var ackMessage AckMessage
  ackMessage.command = command
  ackMessage.host = fromHost
  ackMessage.filename = outputFileList[0]

  ackChannel <- ackMessage

}
