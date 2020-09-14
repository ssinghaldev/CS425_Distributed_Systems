package main

import (
    "bufio"
    "fmt"
    "log"
    "os"
    "path/filepath"
    "regexp"
    "strconv"
    "strings"
    "time"
)

func main() {
    if len(os.Args) < 2 {
        fmt.Printf("Usage: %s <input_file1> <output_file>\n",
                    filepath.Base(os.Args[0]))
        os.Exit(1)
    }

    starttime := time.Now()

    wcarray := map[string]int{}

    readFile(os.Args[1], wcarray)
    filehandle, err := os.OpenFile(os.Args[2], os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
    if err != nil {
        log.Println("Error writing to file: ", err)
    }

    writer := bufio.NewWriter(filehandle)
    for word, count := range wcarray {
        fmt.Fprintln(writer, word+","+strconv.Itoa(count))
    }

    writer.Flush()
    filehandle.Close()


    elapsedtime := time.Since(starttime)
    fmt.Println("Time taken:", elapsedtime)
}

func readFile(filename string, wcarray map[string]int) {

    file, err := os.Open(filename)
    if err != nil {
        log.Println("Error: ", err)
    }

    reg, err := regexp.Compile("[^A-Za-z0-9_]+")
    if err != nil {
        log.Println(err)
    }

    scanner := bufio.NewScanner(file)

    count := 0
    var lineList []string
    for scanner.Scan() {
      line := scanner.Text()
      lineList = append(lineList, line)
      count += 1
      if count == 10{
        mapper(lineList, wcarray, reg)
        count = 0
        lineList = nil
      }
    }

    mapper(lineList, wcarray, reg)
    file.Close()
}

func mapper(lineList []string, wcarray map[string]int, reg *regexp.Regexp){
  for _,line := range(lineList){
    words := strings.Split(line, " ")
    for _, word := range(words){
      word_sanitized := strings.TrimSpace(reg.ReplaceAllString(word, ""))
      if len(word_sanitized) > 0 {
        wcarray[word_sanitized] += 1
      }
    }
  }
}
