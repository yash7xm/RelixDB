package main

import (
	"fmt"
	"log"

	app "github.com/yash7xm/RelixDB/app" // Importing the package directly
)

func main() {
	fmt.Println("Initializing RelixDB...")

	// Create and initialize the database instance
	db := &app.DB{}
	db = db.NewDB("test.db")

	// Open the database
	if err := db.Open(); err != nil {
		log.Fatalf("Error: Unable to open the database: %v", err)
	}
	defer db.Close()

	// Initialize the database schema or perform any setup operations
	app.InitDB(db)

	fmt.Println("RelixDB initialized successfully.")
}
