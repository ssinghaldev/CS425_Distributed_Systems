package main

import(
  "fmt"
  "log"
  //"io/ioutil"
  "net"
  "encoding/json"
  "path/filepath"
  "os"
  "os/exec"
  "strings"
  "strconv"
  "time"
  "bufio"
  "io"
)

/*
func readFilesFromSDFSDir(dirname string) []string{
  var filenames []string

  //files, err := ioutil.ReadDir(getSDFSSubDir() + "/" + dirname)
  files, err := ioutil.ReadDir(LOCAL_FILE_ROOT + "/" + dirname)
  if err != nil{
    log.Printf("Unable to read files from sdfs dir:%s error:%s\n", dirname, err)
    return filenames
  }

  //While giving the files back, we need to give dirname + filename as that is
  //present in SDFS and will be useful for worker process
  for _, file := range files{
    filenames = append(filenames, dirname + "/" + file.Name())
  }

  fmt.Printf("filenames:")
  fmt.Println(filenames)

  return filenames
}*/

//Used for maple/juice tasks to get input files from src dir
func readFilesFromSDFSDir(dirname string) []string{
  var filenames []string
  for sdfsfilename,_ := range myFsServer.FileTable{
    filePath := strings.Split(sdfsfilename, "/")
    if len(filePath) == 2 && filePath[0] == dirname{
      filenames = append(filenames, sdfsfilename)
    }
  }

  fmt.Printf("filenames:")
  fmt.Println(filenames)

  return filenames
}

//Hash partitioning func
func hashPartition(numWorkers int, fileList []string) map[string][]string{
  partitionMap := make(map[string][]string)

  hostList := getLiveServers(numWorkers)
  //hostList = []string{"1", "2", "3", "4", "5"}
  hostListLen := len(hostList)

  for idx, filename:= range fileList{
    hostidx := idx % hostListLen
    partitionMap[hostList[hostidx]] = append(partitionMap[hostList[hostidx]], filename)
  }

  fmt.Printf("partitionMap:")
  fmt.Println(partitionMap)

  return partitionMap

}

//Range Partition func
func rangePartition(numWorkers int, fileList []string) map[string][]string{
  partitionMap := make(map[string][]string)

  hostList := getLiveServers(numWorkers)
  hostListLen := len(hostList)

  for idx, filename:= range fileList{
    hostidx := idx / hostListLen
    partitionMap[hostList[hostidx]] = append(partitionMap[hostList[hostidx]], filename)
  }

  fmt.Printf("partitionMap:")
  fmt.Println(partitionMap)

  return partitionMap

}

func getLiveServers(numWorkers int) []string{
  var hostList []string
  count := 0

  for idx, live := range liveServerBitMap{
    if live == 1 && idx != 0{
      hostList = append(hostList, getHostFromId(idx+1))
      count += 1
    }

    if count == numWorkers{
      break
    }
  }

  fmt.Printf("hostnames:")
  fmt.Println(hostList)

  return hostList
}

func sendMJMessage(msgType string, hostname string, executableCommand string, sdfsfilenames []string){
  var mjMsg MapleJuiceMessage


  mjMsg.Id = msgType
  mjMsg.Data = executableCommand
  mjMsg.Deg = sdfsfilenames

  /*
  mjMsg.messageType = msgType
  mjMsg.executableCommand = executableCommand
  mjMsg.inputFiles = sdfsfilenames*/

  jsonMJMsg :=  getJSONfromMJMessage(mjMsg)
  conn, err := net.Dial("udp", hostname + ":" + "34345")
  if err != nil {
    log.Printf("Unable to connect to SERVER:%s to send %s message. Error:%s\n",
                hostname, msgType, err)
    return
  }
  _, err = conn.Write(jsonMJMsg)
  if err != nil{
    log.Println("Unable to send %s message to SERVER:%s. Error:%s\n",
                 hostname, msgType, err)
  }
}

func isSDFSFilePresentLocally(filePath string) bool{
  sdfsFilePath := getSDFSSubDir() + "/" + filePath
  sdfsFileInfo, err := os.Stat(sdfsFilePath)
  if os.IsNotExist(err) {
      return false
  }
  return !sdfsFileInfo.IsDir()
}

func getFileLocally(filename string) string{
  localfilename := LOCAL_FILE_ROOT + "/" + filepath.Base(filename)

  fmt.Printf("filename:%s localfilename:%s\n", filename, localfilename)
  //Get file locally
  get(myFsServer, filename, localfilename)

  return localfilename
}

func getTmpFilePath() string{
  currTime := strconv.FormatInt(time.Now().Unix(), 10)
  hostID   := strconv.Itoa(getIdFromHost(myFsServer.hostname))
  return LOCAL_FILE_ROOT + "/" + currTime + "_" + hostID

}

func executeProcess(execCommandPath string, inputFilePath string, outputFilePath string){
  _, err := exec.Command(execCommandPath, inputFilePath, outputFilePath).Output()
  if err != nil {
    log.Printf("Unable to execute command:%s on input file:%s. Error:%s\n",
                          execCommandPath, inputFilePath, err)
  }
}

func copyOutputFileToMaster(outputFilePath string) {

  cmd := exec.Command("scp", outputFilePath , INTRODUCER + ":" + outputFilePath)
  err := cmd.Run()
  if err != nil {
    log.Printf("Copying outputFilePath:%s to master failed with %s\n",
                        outputFilePath, err)
    return
  }
}

//Func in master which keeps track of worker progress and rescheduling the failed nodes
func monitorWorkerProgress(cmd MapleJuiceCommand, partitionFiles map[string][]string) []string{

  commandToMonitor := cmd.commandType

 /*
  var hostsToMonitor = make(map[string]int)
  for host, _ := range partitionFiles{
    hostsToMonitor[host] = -1
  }*/

  var processedFiles []string

  numAcksToRecieve := len(partitionFiles)
  numAcksReceived  :=  0
  for numAcksReceived < numAcksToRecieve{
    select{
      case ackMsg := <-ackChannel:
        if ackMsg.command == commandToMonitor {
          if _, ok := partitionFiles[ackMsg.host]; ok{
            fmt.Printf("commandToMonitor:%s ACK Received from %s", commandToMonitor, ackMsg.host)
            processedFiles = append(processedFiles, ackMsg.filename)
            numAcksReceived += 1
           // delete(partitionFiles, ackMsg.host)
          }
        }
      default:
        if isFailed, hostname := checkHostFailure(partitionFiles); isFailed{
          rescheduleTask(cmd, hostname, partitionFiles)
        }
    }
  }

  return processedFiles
}

func checkHostFailure(partitionFiles map[string][]string)(bool, string){
  select{
    case failHost := <-failChannel:
      if _, ok := partitionFiles[failHost];ok{
        fmt.Printf("failed worker:%s Rescheduling the task...", failHost)
        return true, failHost
      }
    default:
      return false, ""
  }

  return false, ""
}

func rescheduleTask(cmd MapleJuiceCommand, failHost string, partitionFiles map[string][]string){

  failHostFileList := partitionFiles[failHost]

  liveServers := getLiveServers(9)
  for _, host := range(liveServers){
    if _, ok := partitionFiles[host]; !ok {
      fmt.Printf("Rescheduled %s task on host:%s\n", cmd.commandType, host)
      partitionFiles[host] = failHostFileList
      fmt.Printf("new file list for new worker %v\n", partitionFiles[host])
      go sendMJMessage(cmd.commandType, host, cmd.commandExec, failHostFileList)
      return
    }
  }

  //Just send it to existing worker
  host := liveServers[0]
  fmt.Printf("Rescheduled %s task on existing worker host:%s\n", cmd.commandType, host)
  partitionFiles[host] = append(partitionFiles[host], failHostFileList...)
  fmt.Printf("new file list for existing worker %v\n", partitionFiles[host])
  go sendMJMessage(cmd.commandType, host, cmd.commandExec, failHostFileList)
}

func processWorkerMapleFiles(processedFiles []string, filenamePrefix string){
  //combinedFile := combineAllProcessedFiles(processedFiles)
  //allKeysHash  := readFileAndCreateHash(combinedFile)
  allKeysHash  := combineAllProcessedFilesAndCreateHash(processedFiles)
  writeKeysHashToSDFSFile(allKeysHash, filenamePrefix)
}

func processWorkerJuiceFiles(processedFiles []string, filename string){
  combinedFilePath := combineAllProcessedFiles(processedFiles)
  put(myFsServer, combinedFilePath, filename)
}

func combineAllProcessedFiles(processedFiles []string) string{
  combinedFile := getTmpFilePath()
  fmt.Printf("combined filepath:%s for juice", combinedFile)

  out, err := os.OpenFile(combinedFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
  if err != nil{
    fmt.Printf("Unable to open combined file:%s\n", combinedFile)
    return ""
  }
  defer out.Close()

  for _, filename := range(processedFiles){
    in, err := os.Open(filename)
    if err != nil{
      fmt.Printf("Unable to open processed file:%s for combining\n", filename)
      continue
    }
    defer in.Close()

    _, err = io.Copy(out, in)
    if err != nil{
      fmt.Printf("Unable to dopy processed file:%s for combining\n", filename)
      continue
    }
  }

  return combinedFile
}

/*
func combineAllProcessedFiles(processedFiles []string) string{
  processedFileString := strings.Join(processedFiles[:], " ")
  combinedFile        := filepath.Dir(processedFiles[0]) + "/processed_combined.csv"

  fmt.Printf("processedFilePath:%s\n", processedFiles[0])
  fmt.Printf("combinedFilePath:%s\n", combinedFile)

  //catCommand := fmt.Sprintf("\"/usr/bin/cat %s > %s\"", processedFileString, combinedFile)
  cmd := exec.Command("cat", processedFileString)
  fmt.Println(cmd)

  var stdout bytes.Buffer
  cmd.Stdout = &stdout

  err := cmd.Run()
  if err != nil{
    fmt.Printf("Unable to execute cat command. Parameters:processedFileString:%s combinedFile:%s. Error:%v\n",
                          processedFileString, combinedFile, err)
    log.Printf("Unable to execute cat command. Parameters:processedFileString:%s combinedFile:%s. Error:%v\n",
                          processedFileString, combinedFile, err)
    return ""
  }

  writeDataToAFile(combinedFile, string(stdout.Bytes()))
  return combinedFile
}*/

func readFileAndCreateHash(combinedFile string) map[string][]string{
  var allKeysHash = make(map[string][]string)

  fmt.Println("In readFileAndCreateHash")
  filehandle, err := os.Open(combinedFile)
  if err != nil{
    fmt.Printf("Unable to open file:%s\n", combinedFile)
    return allKeysHash
  }
  defer filehandle.Close()

  scanner := bufio.NewScanner(filehandle)
  for scanner.Scan(){
    line := scanner.Text()
    key_val := strings.Split(line, ",")
    allKeysHash[key_val[0]] = append(allKeysHash[key_val[0]], key_val[1])
  }
  fmt.Println("Out readFileAndCreateHash")
  return allKeysHash

}

func combineAllProcessedFilesAndCreateHash(processedFiles []string) map[string][]string{
  var allKeysHash = make(map[string][]string)

  fmt.Println("In combineAllProcessedFilesAndCreateHash")

  for _,filename := range(processedFiles){
    filehandle, err := os.Open(filename)
    if err != nil{
      fmt.Printf("Unable to open file:%s\n", filename)
      return allKeysHash
    }
    defer filehandle.Close()

    scanner := bufio.NewScanner(filehandle)
    for scanner.Scan(){
      line := scanner.Text()
      key_val := strings.Split(line, ",")
      allKeysHash[key_val[0]] = append(allKeysHash[key_val[0]], key_val[1])
    }
  }

  fmt.Println("Out combineAllProcessedFilesAndCreateHash")
  return allKeysHash
}

func writeKeysHashToSDFSFile(allKeysHash map[string][]string, filenamePrefix string){
  fmt.Println("In writeKeysHashToSDFSFile")
  CreateDirIfNotExist(LOCAL_FILE_ROOT + "/" + filenamePrefix)
  for key, val := range(allKeysHash){
    filename := LOCAL_FILE_ROOT + "/" + filenamePrefix + "/" + filenamePrefix + "_" + key
    val_str := strings.Join(val[:], ",")
    dataToWrite := key + "," + val_str
    writeDataToAFile(filename, dataToWrite)
    put(myFsServer, filename, filenamePrefix + "/" + filenamePrefix + "_" + key)
    fmt.Printf("intermediate filename:%s written to sdfs\n", filenamePrefix + "/" + filenamePrefix + "_" + key)
  }
  fmt.Println("Out writeKeysHashToSDFSFile")
}

func writeDataToAFile(filename string, dataToWrite string){
    filehandle, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
    if err != nil {
        fmt.Println("Error writing to file:%s  error:%s", filename, err)
        return
    }

    writer := bufio.NewWriter(filehandle)
    fmt.Fprintln(writer, dataToWrite)

    writer.Flush()
    filehandle.Close()
}

func deleteFolder(folder string){
  _, err := exec.Command("rm", "-rf", folder).Output()
  if err != nil {
    log.Printf("Unable to remove intermediate files folder:%s\n",
                          folder)
  }
}

func deleteSDFSDir(sdfsDir string){
  matchedSDFSFiles := readFilesFromSDFSDir(sdfsDir)
  for _, sdfsFile := range(matchedSDFSFiles){
    remove(myFsServer, sdfsFile)
    fmt.Printf("removed sdfsfile:%s\n", sdfsFile)
  }
}

func getMJMessagefromJSON(jsonMessage []byte) MapleJuiceMessage {
  var message MapleJuiceMessage
  err := json.Unmarshal(jsonMessage, &message)
  if err != nil {
    log.Printf("Unable to unmarshal message. Error:%s\n", err)
  }
  return message
}

func getJSONfromMJMessage(message MapleJuiceMessage) []byte {
  var jsonMessage []byte
  jsonMessage, err := json.Marshal(message)
  if err != nil {
    log.Printf("Unable to marshal message. Error:%s\n", err)
  }
  return jsonMessage
}
