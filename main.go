package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

//These are the details about the database connections required
const (
	host     = "localhost"
	port     = 5432
	user     = "postgres"
	password = "password"
	dbname   = "objects"
)

//Below 3 are some structs required in the code -> models
type Objectids struct {
	Ids []int `json:"object_ids"`
}

type Object struct {
	Id     int  `json:"id"`
	Status bool `json:"online"`
}

type databaseObject struct {
	Id        int       `json:"id"`
	Last_seen time.Time `json:"last_seen"`
}

//Below is the worker function that makes a 1 id queue which comes through jobs channel and exits through results channel
//I have purposely made both of them as unidirectional channels to avoid confusion and error
func worker(jobs <-chan int, results chan<- []byte) {
	for n := range jobs {
		results <- getStatus(n)
	}
}

//The below function makes the get request to the tester service and returns the response body
func getStatus(id int) []byte {
	strid := strconv.Itoa(id)
	response, err := http.Get("http://localhost:9010/objects/" + strid)

	if err != nil {
		log.Fatalln(err)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatalln(err)
	}

	return body
}

func callbackHandler(db *sql.DB) func(w http.ResponseWriter, r *http.Request) {

	//The callback handler receives the db connection and the post request and the below function starts processing

	return func(w http.ResponseWriter, r *http.Request) {

		//The post request's body is read here
		reqBody, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s", reqBody)

		//Since this is the time when we receive the post request we mark it as the last seen time for an id
		timestamp := time.Now()

		//The json data received is decoded and stored into struct variable decodedBody
		var decodedBody Objectids
		err = json.Unmarshal([]byte(reqBody), &decodedBody)
		if err != nil {
			log.Fatal(err)
		}

		//Now comes the part of concurrency
		//In the assessment the time interval for the get request for querying the online status of an id varied between 300ms
		//and 4s and the id's were sent every 5 seconds. Moreover the total count of ids could be 200
		//In the worst case scenario if 200 ids were sent it could take upto a maximum of 800s to sent a request for each id
		//and we cannot afford to lose any callbacks.
		//To solve this problem we are making use of go routines and we allocate one go routine to each request and pass it
		//through it's own channel
		//This way no matter how many ids we have we will know the status of all of them in at max 4s before our next
		//batch of ids
		jobs := make(chan int, 1)
		results := make(chan []byte, 1)

		//Here we are allocating a go routine per id
		for i := 0; i < len(decodedBody.Ids); i++ {
			go worker(jobs, results)
		}
		//Here the id enters the job channel
		for i := 0; i < len(decodedBody.Ids); i++ {
			jobs <- decodedBody.Ids[i]
		}
		close(jobs)

		//Here we get back the status of id and do further processing on the basis of that
		for i := 0; i < len(decodedBody.Ids); i++ {
			//Again here we decode the json data we receive from get request into a struct
			var decodedBody Object
			err = json.Unmarshal([]byte(<-results), &decodedBody)
			if err != nil {
				log.Fatalln(err)
			}
			//Here if do the filtering that is if the online status is true we store it otherwise we just let it pass
			if decodedBody.Status == true {
				//Here we are making a query to the database to see if the id is already present there
				//If not present we get an error and we insert the id in the table with the timestamp
				//Otherwise we update the timestamp of the id in the table
				var myObject databaseObject
				sqlStatement := `SELECT id,last_seen FROM object WHERE id = $1`

				err := db.QueryRow(sqlStatement, decodedBody.Id).Scan(&myObject.Id, &myObject.Last_seen)
				if err != nil {
					sqlStatement = `INSERT INTO object (id,last_seen)
				VALUES ($1,$2 )`
					_, err = db.Exec(sqlStatement, decodedBody.Id, timestamp)
					if err != nil {
						panic(err)
					}
				} else {
					sqlStatement := `
	            UPDATE object
				SET last_seen = $2
				WHERE id = $1;`
					_, err = db.Exec(sqlStatement, decodedBody.Id, timestamp)
					if err != nil {
						panic(err)
					}

				}
			}

		}
	}
}

func deleteService(db *sql.DB) {
	//Here we have an infinite loop setup to check the database every second for entries older than 30s
	//and delete them constantly.
	for {
		sqlStatement := `
	DELETE FROM object
	WHERE last_seen < (now()-'30 seconds'::interval)`
		_, err := db.Exec(sqlStatement)
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	//We start from initialising our database
	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}
	fmt.Println("Successfully connected!")

	//This is the delete service that deletes ids older than 30s
	go deleteService(db)

	//The post requests are received on port :9090/callback and they are handled by callback handler then
	//Here I have used a closure to pass in db as a parameterto the handler
	http.HandleFunc("/callback", callbackHandler(db))
	http.ListenAndServe(":9090", nil)
}
