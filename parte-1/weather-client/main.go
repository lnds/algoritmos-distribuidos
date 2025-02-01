package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	. "weather/api"
)

func sequential(key string, cities []string) []*Weather {
	var result []*Weather
	for _, q := range cities {
		r, err := Report(q, key)
		if err != nil {
			log.Fatal(err)
		}
		result = append(result, r)
	}
	return result
}

func fetch(q, key string, ch chan<- *Weather) {
	r, err := Report(q, key)
	if err == nil {
		ch <- r
	}
}

func parallel(key string, cities []string) []*Weather {
	ch := make(chan *Weather)
	for _, q := range cities {
		go fetch(q, key, ch)
	}

	var result []*Weather
	for range cities {
		r := <-ch
		result = append(result, r)
	}
	return result
}

func main() {
	key := os.Getenv("WEATHER_API_KEY")

	start := time.Now()

	var rep []*Weather
	if os.Args[1] == "-p" {
		rep = parallel(key, os.Args[2:])
	} else {
		rep = sequential(key, os.Args[1:])
	}
	printReport(rep)

	duration := time.Since(start)
	fmt.Printf(
		"Elapsed time: %02.0f:%02.0f:%02.3f\n",
		duration.Hours(),
		duration.Minutes(),
		duration.Seconds(),
	)
}

func printReport(report []*Weather) {
	records := [][]string{
		{"City", "Region", "Country", "Condition", "Max Temp", "Min Temp", "Precipitation"},
	}
	for _, r := range report {
		var row []string
		row = append(row, r.Location.Name)
		row = append(row, r.Location.Region)
		row = append(row, r.Location.Country)
		row = append(row, r.Current.Condition.Text)
		if len(r.Forecast.Days) > 0 {
			row = append(row, fmt.Sprintf("%.2f", r.Forecast.Days[0].Day.MaxTemp))
			row = append(row, fmt.Sprintf("%.2f", r.Forecast.Days[0].Day.MinTemp))
			row = append(row, fmt.Sprintf("%.2f", r.Forecast.Days[0].Day.TotalPrecipitation))
		} else {
			row = append(row, []string{"n/a", "n/a", "n/a"}...)
		}
		records = append(records, row)
	}
	w := csv.NewWriter(os.Stdout)
	err := w.WriteAll(records)
	if err != nil {
		log.Fatal(err)
	}
}
