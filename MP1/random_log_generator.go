package main

import ("fmt"
        "os"
        "strings"
        "strconv"
        "math/rand"
)

const junkChars = "nriebmdodyrlPNSVElsi"

func main(){
    if len(os.Args) < 2{
      fmt.Println("Gotta Give some pattern bro :-P")
      fmt.Println("Usage: ./<program> <pattern1,frequency1>")
    }

   //Parse input from user
    s := strings.Split(os.Args[1], ",")
    argument := s[0]
    frequency,_ := strconv.Atoi(s[1])

   //Create a file
    fd, _ := os.Create("random.log")
    defer fd.Close()

    //Writing random string to a file
    maxLines := 50
    for line := 0; line < maxLines; line++{
      if line % frequency == 0{
	fd.WriteString(argument)
        fd.WriteString("\n")
      }else{
	randStr := generateRandStr(rand.Intn(10))
	fd.WriteString(randStr)
        fd.WriteString("\n")
      }
    }
}


//Random string generator func
func generateRandStr(n int) string {
    var ltrs = []rune("nriebmdodyrlPNSVElsi825faijahfjkfnb123gfbjbgf09")
    randomStrOut := make([]rune, n)
    for i := range randomStrOut {
	randomStrOut[i] = ltrs[rand.Intn(len(randomStrOut))]
    }
    return string(randomStrOut)
}
