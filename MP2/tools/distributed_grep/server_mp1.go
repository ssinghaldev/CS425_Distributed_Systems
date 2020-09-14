package main

import (
	"fmt"
        "bufio"
	"net"
	"os"
        "os/exec"
)

//Gotta declare global constants man :-)
const PortNumber = "27001"

func main() {
  //checking command line args
  if len(os.Args) != 2{
    fmt.Println("Usage: ./server <log_file_name>")
    os.Exit(1)
  }

  fileName := os.Args[1]
  server, listenError := net.Listen("tcp", ":" + PortNumber)
  if listenError != nil {
    fmt.Println("Unable to start server: ", listenError)
    os.Exit(1)
  }
  defer server.Close()

  //Infinite loop to accept connections and then find the lines with matched pattern
  for {
    connection, acceptError := server.Accept()
    if acceptError != nil {
      fmt.Println("Unable to accept Connection: ", acceptError)
      os.Exit(1)
    }

    //spawing a goroutine to handle incoming connection
    go searchPatternAndSendOutputToClient(connection, fileName)
  }
}

//find the matched lines and send output back to client
func searchPatternAndSendOutputToClient(connection net.Conn, fileName string) {
  //Ain't gonna leak the fd :-P
  defer connection.Close()

  readFromClient := bufio.NewReader(connection)
  patternFromClient, readError := readFromClient.ReadString('\n')
  if readError != nil{
    fmt.Println("Unable to read pattern from client: ", readError)
  }
  
  //removing the last '\n' character from pattern
  patternFromClient = patternFromClient[:len(patternFromClient)-1]

  //executing a sub-process (grep) and getting matched lines and their line numbers
  grepCommand := exec.Command("/bin/grep", "-nEe", patternFromClient, fileName)
  matchedLines, stderr := grepCommand.CombinedOutput()
  if stderr != nil{
    fmt.Println("Unable to receive output while running grep: ", stderr)
    return
  }

  //finally writing back to client
  connection.Write([]byte(matchedLines))
  return
}
