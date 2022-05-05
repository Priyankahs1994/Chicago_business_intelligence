package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"
	"os"
	"log"
	"database/sql"
	"encoding/json"
	
	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type TaxiTripsDataJsonRecords []struct {
	Taxi_Trip_id                    string `json:"trip_id"`
	Dropoff_latitude  				string `json:"dropoff_centroid_latitude"`
	Dropoff_longitude 				string `json:"dropoff_centroid_longitude"`
	Dropoff_area	 				string `json:"dropoff_community_area"`
}

type BuildingPermitsDataJsonRecords []struct {
	Permit_Id							string `json:"permit_"`
	Date_Of_Issue 				   		string `json:"issue_date"`
}

type CovidTestDataJsonRecords []struct {
	Covid_Data_Collection_Date			string `json:"date"`
	Total_People_Tested     			string `json:"people_tested_total"`
	Total_People_Positive   			string `json:"people_positive_total"`
	Total_People_Negative  				string `json:"people_not_positive_total"`
}

type UnemploymentRatesDataJsonRecords []struct {
	Community_Area					string `json:"community_area"`
	Income     						string `json:"per_capita_income"`
	Unemployment_Rate   			string `json:"unemployment"`
}


func main() {

	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/clean-linker-349000:us-central1:mypostgres sslmode=disable port = 5432"


	db, err := sql.Open("postgres", db_connection)
	if err != nil {
		panic(err)
	}

	for {

		GetUnemploymentRatesData(db)
		GetCovidTestData(db)
		GetBuildingPermitsData(db)
		GetTaxiTripsData(db)

		port := os.Getenv("PORT")
			if port == "" {
        	port = "8080"
		}
		log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", port), nil))

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		time.Sleep(24 * time.Hour)
	}

}

func GetTaxiTripsData(db *sql.DB) {

	geocoder.ApiKey = "AIzaSyDaN16lysuWJvIrpEqXV5PxKOxb-IWKmtg"


	drop_table := `drop table if exists taxi_trips_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "taxi_trips_data" (
						"taxi_trip_id"   SERIAL , 
						"dropoff_latitude" DOUBLE PRECISION, 
						"dropoff_longitude" DOUBLE PRECISION,
						"dropoff_area" INT, 
						"dropoff_zipCode" VARCHAR(255), 
						PRIMARY KEY ("taxi_trip_id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=100"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_data_list TaxiTripsDataJsonRecords
	json.Unmarshal(body, &taxi_trips_data_list)

	for i := 0; i < len(taxi_trips_data_list); i++ {

		dropoff_latitude := taxi_trips_data_list[i].Dropoff_latitude

		if dropoff_latitude == "" {
			continue
		}

		dropoff_longitude := taxi_trips_data_list[i].Dropoff_longitude

		if dropoff_longitude == "" {
			continue
		}

		dropoff_area := taxi_trips_data_list[i].Dropoff_area

		if dropoff_area == "" {
			continue
		}


		dropoff_latitude_float, _ := strconv.ParseFloat(dropoff_latitude, 64)
		dropoff_longitude_float, _ := strconv.ParseFloat(dropoff_longitude, 64)

		dropoff_loc := geocoder.Location{
			Latitude:  dropoff_latitude_float,
			Longitude: dropoff_longitude_float,
		}

		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_loc)
		dropoff_address := dropoff_address_list[0]
		dropoff_zipCode := dropoff_address.PostalCode

		sql := `INSERT INTO taxi_trips_data ("dropoff_latitude", "dropoff_longitude", "dropoff_area", "dropoff_zipCode") values($1, $2, $3, $4)`

		_, err = db.Exec(
			sql,
			dropoff_latitude,
			dropoff_longitude,
			dropoff_area,
			dropoff_zipCode)

		if err != nil {
			panic(err)
		}
	}

}

func GetUnemploymentRatesData(db *sql.DB) {

	drop_table := `drop table if exists unemployment_rates_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "unemployment_rates_data" (
						"id"   SERIAL , 
						"community_area" 	VARCHAR(255) UNIQUE,
						"income" VARCHAR(255), 
						"unemployment_rate" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_rates_data_list UnemploymentRatesDataJsonRecords
	json.Unmarshal(body, &unemployment_rates_data_list)

	for i := 0; i < len(unemployment_rates_data_list); i++ {

		community_area := unemployment_rates_data_list[i].Community_Area
		if community_area == "" {
			continue
		}

		income := unemployment_rates_data_list[i].Income
		if income == "" {
			continue
		}

		unemployment_rate := unemployment_rates_data_list[i].Unemployment_Rate
		if unemployment_rate == "" {
			continue
		}

		sql := `INSERT INTO unemployment_rates_data("community_area", "income", "unemployment_rate") values($1, $2, $3)`

		_, err = db.Exec(
			sql,
			community_area,
			income,
			unemployment_rate)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("GetUnemploymentRates: Implement Unemployment")
}

func GetBuildingPermitsData(db *sql.DB) {

	geocoder.ApiKey = "AIzaSyDaN16lysuWJvIrpEqXV5PxKOxb-IWKmtg"


	drop_table := `drop table if exists building_permits_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permits_data" (
						"permit_id"   SERIAL , 
						"date_of_issue" TIMESTAMP WITH TIME ZONE,
						PRIMARY KEY ("permit_id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	var url = "https://data.cityofchicago.org/resource/ydr8-5enu.json"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var building_permits_data_list BuildingPermitsDataJsonRecords
	json.Unmarshal(body, &building_permits_data_list)

	for i := 0; i < len(building_permits_data_list); i++ {

		date_of_issue := building_permits_data_list[i].Date_Of_Issue
		if len(date_of_issue) < 23 {
			continue
		}

		sql := `INSERT INTO building_permits_data ("date_of_issue") values($1)`

		_, err = db.Exec(
			sql,
			date_of_issue)

		if err != nil {
			panic(err)
		}
	}
}

func GetCovidTestData(db *sql.DB) {

	drop_table := `drop table if exists covid_test_data`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "covid_test_data" (
						"id"   SERIAL,
						"covid_data_collection_date" TIMESTAMP WITH TIME ZONE,
						"total_people_tested" INT,
						"total_people_positive" INT,
						"total_people_negative" INT,
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	var url = "https://data.cityofchicago.org/resource/t4hh-4ku9.json"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	body, _ := ioutil.ReadAll(res.Body)
	var covid_test_data_list CovidTestDataJsonRecords
	json.Unmarshal(body, &covid_test_data_list)

	for i := 0; i < len(covid_test_data_list); i++ {

		covid_data_collection_date := covid_test_data_list[i].Covid_Data_Collection_Date
		if len(covid_data_collection_date) < 23 {
			continue
		}

		total_people_tested := covid_test_data_list[i].Total_People_Tested
		if total_people_tested == "" {
			continue
		}

		total_people_positive := covid_test_data_list[i].Total_People_Positive
		if total_people_positive == "" {
			continue
		}

		total_people_negative := covid_test_data_list[i].Total_People_Negative
		if total_people_negative == "" {
			continue
		}

		sql := `INSERT INTO covid_test_data ("covid_data_collection_date","total_people_tested", "total_people_positive", "total_people_negative") values($1, $2, $3, $4)`

		_, err = db.Exec(
			sql,
			covid_data_collection_date,
			total_people_tested,
			total_people_positive,
			total_people_negative)

		if err != nil {
			panic(err)
		}
	}

	fmt.Println("GetCovidTestData: Implementing Covid Test Data")
}
