package main

import (
	"L0/Structure"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/nats-io/stan.go"
	"net/http"
	"text/template"
	"time"
)

// DB connection params
const (
	host     = "localhost"
	port     = 5432
	user     = "ion_mion"
	password = "qwerty"
	dbname   = "postgres"
)

// Global cache
var (
	cache = make(map[string]Structure.Order)
)

// Handler for index page
func index(w http.ResponseWriter, r *http.Request) {
	t, err := template.ParseFiles("./Subscriber/index.html", "./Subscriber/header.html",
		"./Subscriber/footer.html")

	if err != nil {
		fmt.Fprintf(w, err.Error())
	}

	t.ExecuteTemplate(w, "index", nil)
}

// Handler for result page
func result(w http.ResponseWriter, r *http.Request) {
	track := r.FormValue("track")

	t, err := template.ParseFiles("./Subscriber/header.html", "./Subscriber/result.html",
		"./Subscriber/footer.html")

	if err != nil {
		fmt.Fprintf(w, err.Error())
	}

	if cache[track].TrackNumber == "" {
		t.ExecuteTemplate(w, "index", "Такого заказа не существует! Попробуйте снова!")
	} else {

		output, err := json.MarshalIndent(cache[track], "", "    ")

		if err != nil {
			fmt.Fprintf(w, err.Error())
		}

		t.ExecuteTemplate(w, "index", string(output))
	}
}

// Global handler
func handleFunc() {
	http.HandleFunc("/", index)
	http.HandleFunc("/result", result)
	http.ListenAndServe(":8080", nil)
}

// Insert recieved data to database
func writeDataDb(db *sql.DB, data Structure.Order) {
	deliveryJson, _ := json.Marshal(data.Delivery)
	_, err := db.Exec("INSERT INTO Orders VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
		data.OrderUid, data.TrackNumber, data.Entry, deliveryJson, data.Locale, data.InternalSignature,
		data.CustomerId, data.DeliveryService, data.Shardkey, data.SmId, data.DateCreated, data.OofShard)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec("INSERT INTO Payment VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
		data.Payment.Transaction, data.Payment.RequestId, data.Payment.Currency, data.Payment.Provider,
		data.Payment.Amount, data.Payment.PaymentDt, data.Payment.Bank, data.Payment.DeliveryCost,
		data.Payment.GoodsTotal, data.Payment.CustomFee)
	if err != nil {
		panic(err)
	}
	for _, item := range data.Items {
		_, err = db.Exec("INSERT INTO Items VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
			item.ChrtId, item.TrackNumber, item.Price, item.Rid, item.Name, item.Sale, item.Size, item.TotalPrice,
			item.NmId, item.Brand, item.Status)
		if err != nil {
			panic(err)
		}
	}
}

// Restore cache from database
func restoreCacheDb(db *sql.DB) {

	rows, err := db.Query("SELECT * FROM Orders " +
		"INNER JOIN Payment ON orders.order_uid = payment.transaction;")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var data Structure.Order
		var temp []uint8

		err := rows.Scan(&data.OrderUid, &data.TrackNumber, &data.Entry, &temp,
			&data.Locale, &data.InternalSignature, &data.CustomerId, &data.DeliveryService,
			&data.Shardkey, &data.SmId, &data.DateCreated, &data.OofShard, &data.Payment.Transaction,
			&data.Payment.RequestId, &data.Payment.Currency, &data.Payment.Provider, &data.Payment.Amount,
			&data.Payment.PaymentDt, &data.Payment.Bank, &data.Payment.DeliveryCost, &data.Payment.GoodsTotal,
			&data.Payment.CustomFee)
		if err != nil {
			panic(err)
		}

		json.Unmarshal(temp, &data.Delivery)

		items, err := db.Query("SELECT * FROM Items "+
			"WHERE items.track_number = $1", data.TrackNumber)
		if err != nil {
			panic(err)
		}

		i := 0
		for items.Next() {
			data.Items = append(data.Items, Structure.Item{
				ChrtId:      0,
				TrackNumber: "",
				Price:       0,
				Rid:         "",
				Name:        "",
				Sale:        0,
				Size:        "",
				TotalPrice:  0,
				NmId:        0,
				Brand:       "",
				Status:      0,
			})
			err := items.Scan(&data.Items[i].ChrtId, &data.Items[i].TrackNumber, &data.Items[i].Price,
				&data.Items[i].Rid, &data.Items[i].Name, &data.Items[i].Sale, &data.Items[i].Size,
				&data.Items[i].TotalPrice, &data.Items[i].NmId, &data.Items[i].Brand, &data.Items[i].Status)
			if err != nil {
				panic(err)
			}
			i++
		}
		cache[data.TrackNumber] = data
	}
}

func main() {

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	// Connect to database
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic("Your psql params is wrong")
	}

	// Close connection after end of main function
	defer db.Close()

	// Open up connection
	err = db.Ping()
	if err != nil {
		panic("Can't connect to database")
	}

	// Restore cache from database
	restoreCacheDb(db)

	// Run http server
	go handleFunc()

	// Connect to nats-streaming-server
	sc, err := stan.Connect("prod", "sub")
	// Handle error of connection
	if err != nil {
		panic("Can't connect to nats-streaming server")
	}

	// Close connection after end of main function
	defer sc.Close()

	// Make subscription
	sc.Subscribe("json_data", func(m *stan.Msg) { // Get data
		fmt.Println("-------NEW INCOMING DATA-------")
		var data Structure.Order
		if err := json.Unmarshal(m.Data, &data); err != nil {
			fmt.Println("Can't parse incoming data")
		} else {
			fmt.Println("Get data parse correctly")
			// Check data
			flag := true
			for _, el := range cache {
				if el.OrderUid == data.OrderUid { // Uniq order uid
					flag = false
					break
				} else if el.Payment.Transaction == data.Payment.Transaction { // Uniq payment transaction
					flag = false
					break
				} else if el.TrackNumber == data.TrackNumber { // Uniq track number
					flag = false
					break
				}
			}
			if data.OrderUid != data.Payment.Transaction { // Same order uid and payment transaction
				flag = false
			}
			for _, item := range data.Items { // Same item track number and orders track number
				if item.TrackNumber != data.TrackNumber {
					flag = false
					break
				}
				// Here need to make check for uniq id in items table
				// --------------------------------------------------
				// --------------------------------------------------
				// --------------------------------------------------
			}
			if flag {
				// Append to cache
				cache[data.TrackNumber] = data
				// Write to database
				writeDataDb(db, data)
			} else {
				fmt.Println("Something wrong with data, database not affected")
			}
		}
	}, stan.DurableName("my-durable"))

	// Service life-time
	time.Sleep(time.Hour)
}
