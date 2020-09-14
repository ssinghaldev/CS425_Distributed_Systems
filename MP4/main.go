package main

import (
  "fmt"
  "log"
  "bufio"
  "strings"
  "strconv"
  "os"
  "path/filepath"
)

//Making server Global as we require it in maple juice
//Should think of some other alternative
var myFsServer *FileSystemServer

func main(){
  InitializeSystem()
}

func InitializeSystem(){

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
  myFsServer = InitMyFileServer(hostname)
  go fsCommandListener(myFsServer)
  go commandListener(myServer)

  //initialising maple-juice system
  go mjCommandListener(hostname, DEFAULT_PORT_TO_MJ_LISTEN)
  go processMapleJuiceCommand() //TODO: Do this only for master

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
        c. Give maple command
        d. Give juice command

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
      case 'c':
        fmt.Printf("Enter the command as : maple <maple_exe> <num_maples>" +
                    " <sdfs_intermediate_file_prefix> <sdfs_src_directory>\n")

        command, readError := commandReader.ReadString('\n')
        command, readError = commandReader.ReadString('\n')
        if readError != nil{
          log.Printf("Unable to read user input. Error:%s\n", readError)
        }

        command = strings.TrimSpace(command)
        command_args  := strings.Split(command, " ")
        if len(command_args) != 5{
          fmt.Printf("Incorrect Format. Please see the format printed above\n")
        }else{
          var mjCommand MapleJuiceCommand
          mjCommand.commandType = command_args[0]
          mjCommand.commandExec = command_args[1]
          mjCommand.numWorkers, _ = strconv.Atoi(command_args[2])
          mjCommand.sdfsSrcDir = command_args[4]
          mjCommand.sdfsIntermediateFilenamePrefix = command_args[3]

          commandQueue <- mjCommand
        }

      case 'd':
        fmt.Printf("Enter the command as : juice <juice_exe> <num_juices>" +
                    " <sdfs_intermediate_file_prefix> <sdfs_dest_filename>" +
                    " delete_input={0,1} partition={0,1}\n")
        command, readError := commandReader.ReadString('\n')
        command, readError = commandReader.ReadString('\n')
        if readError != nil{
          log.Printf("Unable to read user input. Error:%s\n", readError)
        }

        command = strings.TrimSpace(command)
        command_args  := strings.Split(command, " ")
        if len(command_args) != 7{
          fmt.Printf("Incorrect Format. Please see the format printed above\n")
        }else{
          var mjCommand MapleJuiceCommand
          mjCommand.commandType = command_args[0]
          mjCommand.commandExec = command_args[1]
          mjCommand.numWorkers, _ = strconv.Atoi(command_args[2])
          mjCommand.sdfsSrcDir = command_args[3]
          mjCommand.sdfsDestFilename = command_args[4]
          mjCommand.deleteInput,_ = strconv.Atoi(command_args[5])
          mjCommand.partitionType,_ = strconv.Atoi(command_args[6])

          commandQueue <- mjCommand
        }
    }
  }
}
