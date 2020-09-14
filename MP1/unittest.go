package main

import(
  "fmt"
  "net"
  "bufio"
  "strings"
)

func main(){

  //Test for frequent pattern
  frequentPatternTest()

  //Test for someWhatFrequent pattern
  someWhatFrequentPatternTest()

  //Test for rare pattern
  rarePatternTest()

  //Test for patten from one server
  patternFromOneServerTest()

  //Test for pattern from some servers
  patternFromSomeServersTest()

  //Test for pattern from all servers
  patternFromAllServersTest()
}

func frequentPatternTest(){
  fmt.Println("--------- TestCase : frequentPatternTest ----------------")

  numLinesExpected := 13
  numLinesMatched := connectToServerAndGetNumLinesMatched("172.22.152.103:27001", "shubham")
  fmt.Println(numLinesMatched)
  if numLinesMatched == numLinesExpected{
    fmt.Println("Success: Matching the expected frequent pattern count")
  }else{
    fmt.Println("Failure: Oops! Line count didn't match expected result. Please check!")
  }
}

func someWhatFrequentPatternTest(){
  fmt.Println("--------- TestCase : someWhatFrequentPatternTest ----------------")

  numLinesExpected := 4
  numLinesMatched := connectToServerAndGetNumLinesMatched("172.22.152.103:27001", "Nirupam")
  if numLinesMatched == numLinesExpected{
    fmt.Println("Success: Matching the expected frequent pattern count")
  }else{
    fmt.Println("Failure: Oops! Line count didn't match expected result. Please check!")
  }
}

func rarePatternTest(){
  fmt.Println("--------- TestCase : rarePatternTest ----------------")

  numLinesExpected := 1
  numLinesMatched := connectToServerAndGetNumLinesMatched("172.22.152.103:27001", "bla")
  if numLinesMatched == numLinesExpected{
    fmt.Println("Success: Matching the expected rare  pattern count")
  }else{
    fmt.Println("Failure: Oops! Line count didn't match expected result. Please check!")
  }
}

func patternFromOneServerTest(){
  fmt.Println("--------- TestCase : patternFromOneServerTest ----------------")

  matchedPatternExpectedServerCount := 1
  matchedPatternServerCount := connectToServersAndGetNumServersMatched("bla")

  if matchedPatternServerCount == matchedPatternExpectedServerCount{
    fmt.Println("Sucess: Matched pattern in only one server!")
  }else{
    fmt.Println("Failure: Matched pattern in multiple servers!")
  }
}

func patternFromSomeServersTest(){
  fmt.Println("--------- TestCase : patternFromSomeServersTest ----------------")


  matchedPatternExpectedServerCount := 2
  matchedPatternServerCount := connectToServersAndGetNumServersMatched("Nirupam")

  if matchedPatternServerCount == matchedPatternExpectedServerCount{
    fmt.Println("Sucess: Matched pattern in some servers!")
  }else{
    fmt.Println("Failure: Didn't match pattern in some servers!")
  }
}

func patternFromAllServersTest(){
  fmt.Println("--------- TestCase : patternFromAllServersTest ----------------")


  matchedPatternExpectedServerCount := 4
  matchedPatternServerCount := connectToServersAndGetNumServersMatched("shubham")

  if matchedPatternServerCount == matchedPatternExpectedServerCount{
    fmt.Println("Sucess: Matched pattern in all servers!")
  }else{
    fmt.Println("Failure: Didn't match pattern in all servers!")
  }
}

func connectToServerAndGetNumLinesMatched(hostname string, pattern string) int{
  matchedOutput := sendPatternToServerAndGetMatchedLines(hostname, pattern)
  
  //decrementing 1 as the trailing new line is also counted in matchedoutput
  return len(matchedOutput) - 1
}

func connectToServersAndGetNumServersMatched(pattern string) int{
  servers := []string{"172.22.152.103:27001",
                      "172.22.154.99:27001",
                      "172.22.156.99:27001",
                      "172.22.154.100:27001"}

  matchedPatternServerCount := 0
  for _, server := range servers{
    matchedOutput := sendPatternToServerAndGetMatchedLines(server, pattern)
    
    //first condition is to not consider the servers  where we didn't find the pattern
    //this is done as we return empty string array of len 1 from those servers
    if strings.TrimSpace(strings.Join(matchedOutput,"")) != "" && len(matchedOutput) > 0{
      matchedPatternServerCount += 1
    }
  }

  return matchedPatternServerCount
}

func sendPatternToServerAndGetMatchedLines(hostname string, pattern string) []string {
  connection, err := net.Dial("tcp", hostname)
  if err != nil {
    errorMsg := []string{"Couldn't connect to host: " + hostname}
    return errorMsg
  }
  defer connection.Close()

  fmt.Fprintf(connection, pattern + "\n")

  fmt.Println("Matched Results from Host: " + hostname)
  matchedResults := make([]string, 1)
  scanner := bufio.NewScanner(connection)
  for scanner.Scan(){
    matchedResults = append(matchedResults, scanner.Text())
  }
  fmt.Println(matchedResults) 
    return matchedResults
}
