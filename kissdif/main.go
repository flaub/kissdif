package main

import (
	"fmt"
	_ "github.com/flaub/kissdif/driver/mem"
	_ "github.com/flaub/kissdif/driver/sql"
	"github.com/flaub/kissdif/server"
)

func main() {
	fmt.Println("KISS Data Interface")
	srv := server.NewServer()
	srv.ListenAndServe()
}
