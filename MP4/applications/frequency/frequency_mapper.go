package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
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

	//readFile(os.Args[1], wcarray)
	filehandle, err := os.OpenFile(os.Args[2], os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		log.Println("Error writing to file: ", err)
	}

	file, err1 := os.Open(os.Args[1])
	if err1 != nil {
		log.Println("Error: ", err1)
	}

	reg := regexp.MustCompile(`https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+`)

	writer := bufio.NewWriter(filehandle)
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

        for key,val := range(wcarray){
		s := fmt.Sprintf("%s#%d",key,val)
		fmt.Fprintln(writer, "1,"+s)
	}

	file.Close()

	writer.Flush()
	filehandle.Close()

	elapsedtime := time.Since(starttime)
	fmt.Println("Time taken:", elapsedtime)
}

func mapper(lineList []string, wcarray map[string]int, reg *regexp.Regexp){
  for _,line := range(lineList){
    submatchall := reg.FindAllString(line, -1)
    for _, element := range submatchall {
            wcarray[element] += 1
    }
  }
}
