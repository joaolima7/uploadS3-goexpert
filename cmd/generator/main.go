package main

import (
	"fmt"
	"os"
)

func main() {
	for i := 0; i <= 100; i++ {
		f, err := os.Create(fmt.Sprintf("./tmp/file%d.txt", i))
		if err != nil {
			panic(err)
		}

		defer f.Close()
		f.WriteString("Hello World!")
	}
}
