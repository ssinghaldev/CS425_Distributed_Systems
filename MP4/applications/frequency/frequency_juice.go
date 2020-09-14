package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
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
        
	file, err := os.Open(os.Args[1])
        if err != nil {
                log.Println("Error: ", err)
        }
	
	fmt.Println("Before reading")
        scanner := bufio.NewScanner(file)
        for scanner.Scan() {
		fmt.Println("just after scanner")
                word := scanner.Text()
		fmt.Println(word)
                if len(word) > 0 {
                        split := strings.Split(word, ",")
                        
			for idx, key_val := range(split){
				if idx == 0{
					continue
				}
				key_val_arr := strings.Split(key_val, "#")
				val, _ := strconv.Atoi(key_val_arr[1])
				wcarray[key_val_arr[0]] += val
			}
                }
		
        }
        file.Close()

	var sum1 = 0
	for _, count := range wcarray {
		sum1 = sum1 + count
	}
	fmt.Println(sum1)
	
	filehandle, err := os.OpenFile(os.Args[2], os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		log.Println("Error writing to file: ", err)
	}
	writer := bufio.NewWriter(filehandle)
	
	for word, count := range wcarray {
		fmt.Fprintln(writer, word+" , "+strconv.Itoa(count))
		fmt.Printf("%s  %s\n", word, strconv.Itoa(count))
		fmt.Fprintln(writer, "Frequency of "+word+" , "+strconv.FormatFloat(float64(count)/float64(sum1), 'f', -1, 64))
		fmt.Printf("frequency:  %s  %f\n", word, float32(count)/float32(sum1))
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
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		word := scanner.Text()
		if len(word) > 0 {
			split := strings.Split(word, ",")
			key := split[0]
			for idx, in := range split {
				if idx == 0 {
					continue
				} else {
					value, _ := strconv.Atoi(in)
					wcarray[key] += value
				}
			}
		}
	}

	file.Close()
}

