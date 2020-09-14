package main

import (
	"fmt"
	"bufio"
        "net"
	"os"
	"strings"
        "strconv"
)

//Useful for printing matched lines
var serverIpToLogFileMap = map[string]string{
					  "172.22.152.103":"mp2.log",
					  "172.22.154.99":"mp2.log",
					  "172.22.156.99":"mp2.log",
					  "172.22.152.104":"mp2.log",
					  "172.22.154.100":"mp2.log",
					  "172.22.156.100":"mp2.log",
					  "172.22.152.105":"mp2.log",
					  "172.22.154.101":"mp2.log",
					  "172.22.156.101":"mp2.log"}

func main() {

  //Checking command-line args
  if len(os.Args) != 2{
    fmt.Println("Usage: ./client <regex_pattern>")
    os.Exit(1)
  }

  regexPattern := os.Args[1]
  servers := []string{"172.22.154.99:27001", 
                      "172.22.152.103:27001", 
                      "172.22.156.99:27001",
                      "172.22.152.104:27001",
                      "172.22.154.100:27001",
                      "172.22.156.100:27001",
                      "172.22.152.105:27001",
                      "172.22.154.101:27001",
                      "172.22.156.101:27001"}
  channel := make(chan []string)

  //parallely calling servers and getting the matched lines
  for _, server := range servers{
    go sendPatternToServerAndGetMatchedLines(server, regexPattern, channel)
  }

  //printing matched lines
  for i := 0; i < len(servers); i++{
    matchedOutput := <-channel
    for _, msg := range matchedOutput[1:] {
      fmt.Println(msg)
    }
  }
}

// Connects to server and get matched lines
func sendPatternToServerAndGetMatchedLines(hostname string, pattern string, channel chan []string){
  connection, connError := net.Dial("tcp", hostname)
  if connError != nil {
    errorMessage := make([]string, 1)
    errorMessage = append(errorMessage, "Couldn't connect to host: " + hostname)
    channel <- errorMessage
    return
  }
  defer connection.Close()

 //Sending the pattern to server
 fmt.Fprintf(connection, pattern + "\n")
  
 matchedResults := make([]string, 1)

 //Take only serverIp from  serverIp:port_number
 serverIp := strings.Split(hostname, ":")[0]
 numLines := 0
 
 //Getting the results line by line and appending to a string array as channel takes string array
 //Also appending details such as IP, log-file name from which it is matched
 scanner := bufio.NewScanner(connection)
 for scanner.Scan(){
   matchedResults = append(matchedResults, serverIp + " " + serverIpToLogFileMap[serverIp] + " " + scanner.Text())
   numLines += 1
 }
 matchedResults = append(matchedResults, "Total Lines Matched in " + serverIp + " " + serverIpToLogFileMap[serverIp] + 
                         " is " + strconv.Itoa(numLines))
 
 channel <- matchedResults
 return
}
