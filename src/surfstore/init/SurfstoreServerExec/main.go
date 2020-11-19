package main

import (
	"log"
	"surfstore"
)

func main() {
	serverInstance := surfstore.NewSurfstoreServer()
	log.Println(surfstore.ServeSurfstoreServer("localhost:8080", serverInstance))
}
