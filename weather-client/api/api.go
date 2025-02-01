package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Location struct {
	Name      string  `json:"name"`
	Region    string  `json:"region"`
	Country   string  `json:"country"`
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lon"`
	TimeZone  string  `json:"tz_id"`
	LocalTime string  `json:"localtime"`
}

type Condition struct {
	Text string `json:"text"`
	Icon string `json:"icon"`
}

type Current struct {
	TimeStamp            string    `json:"last_updated"`
	Temperature          float64   `json:"temp_c"`
	IsDay                int       `json:"is_day"`
	Condition            Condition `json:"condition"`
	WindSpeed            float64   `json:"wind_kph"`
	WindDegree           int       `json:"wind_degree"`
	WindDirection        string    `json:"wind_dir"`
	Pressure             float64   `json:"pressure_mb"`
	Precipitation        float64   `json:"precip_mm"`
	Humidity             float64   `json:"humidity"`
	Cloud                float64   `json:"cloud"`
	FeelsLike            float64   `json:"feelslike_c"`
	UltraVioletRadiation float64   `json:"uv"`
}

type Day struct {
	MaxTemp              float64   `json:"maxtemp_c"`
	MinTemp              float64   `json:"mintemp_c"`
	MaxWind              float64   `json:"maxwind_kph"`
	TotalPrecipitation   float64   `json:"totalprecip_mm"`
	Humidity             float64   `json:"avghumidity"`
	Condition            Condition `json:"condition"`
	UltraVioletRadiation float64   `json:"uv"`
}

type DailyForecast struct {
	Date string `json:"date"`
	Day  Day    `json:"day"`
}

type Forecast struct {
	Days []DailyForecast `json:"forecastday"`
}

type Weather struct {
	Location Location `json:"location"`
	Current  Current  `json:"current"`
	Forecast Forecast `json:"forecast"`
}

func Report(q, key string) (*Weather, error) {
	url := fmt.Sprintf("https://api.weatherapi.com/v1/forecast.json?q=%s&key=%s", q, key)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var report Weather
	err = json.Unmarshal(body, &report)
	if err != nil {
		return nil, err
	}
	return &report, nil
}
