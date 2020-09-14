package main

import (
    "bufio"
    "fmt"
    "log"
    "os"
    "path/filepath"
//    "regexp"
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
    filehandle, err := os.OpenFile(os.Args[2], os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
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

    /*
    reg, err := regexp.Compile("[^A-Za-z0-9]+")
    if err != nil {
        log.Println(err)
    }*/

    scanner := bufio.NewScanner(file)
    scanner.Split(bufio.ScanWords)
    for scanner.Scan() {
        //word := strings.TrimSpace(reg.ReplaceAllString(scanner.Text(), ""))
        word := scanner.Text()
        if len(word) > 0 {
            split := strings.Split(word, ",")
            key := split[0]
            for idx, in := range(split){
              if idx == 0{
                continue
              }else{
                value,_ := strconv.Atoi(in)
                wcarray[key] += value
              }
            }
        }
    }

    file.Close()
}
