// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package dailytest

import (
	"fmt"
	"math/rand"
	"time"
)

const (
	alphabet       = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	yearFormat     = "2006"
	dateFormat     = "2006-01-02"
	timeFormat     = "15:04:05"
	dateTimeFormat = "2006-01-02 15:04:05"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min+1)
}

func randInt64(min int64, max int64) int64 {
	return min + rand.Int63n(max-min+1)
}

func randString(n int) string {
	bytes := make([]byte, n)
	for i := range bytes {
		bytes[i] = alphabet[randInt(0, len(alphabet)-1)]
	}
	return string(bytes)
}

func randDate(min string, max string) string {
	if len(min) == 0 {
		year := time.Now().Year()
		month := randInt(1, 12)
		day := randInt(1, 28)
		return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	}

	minTime, _ := time.Parse(dateFormat, min)
	if len(max) == 0 {
		t := minTime.Add(time.Duration(randInt(0, 365)) * 24 * time.Hour)
		return fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
	}

	maxTime, _ := time.Parse(dateFormat, max)
	days := int(maxTime.Sub(minTime).Hours() / 24)
	t := minTime.Add(time.Duration(randInt(0, days)) * 24 * time.Hour)
	return fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
}

func randTime(min string, max string) string {
	if len(min) == 0 || len(max) == 0 {
		hour := randInt(0, 23)
		min := randInt(0, 59)
		sec := randInt(0, 59)
		return fmt.Sprintf("%02d:%02d:%02d", hour, min, sec)
	}

	minTime, _ := time.Parse(timeFormat, min)
	maxTime, _ := time.Parse(timeFormat, max)
	seconds := int(maxTime.Sub(minTime).Seconds())
	t := minTime.Add(time.Duration(randInt(0, seconds)) * time.Second)
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second())
}

func randTimestamp(min string, max string) string {
	if len(min) == 0 {
		year := time.Now().Year()
		month := randInt(1, 12)
		day := randInt(1, 28)
		hour := randInt(0, 23)
		min := randInt(0, 59)
		sec := randInt(0, 59)
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec)
	}

	minTime, _ := time.Parse(dateTimeFormat, min)
	if len(max) == 0 {
		t := minTime.Add(time.Duration(randInt(0, 365)) * 24 * time.Hour)
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
	}

	maxTime, _ := time.Parse(dateTimeFormat, max)
	seconds := int(maxTime.Sub(minTime).Seconds())
	t := minTime.Add(time.Duration(randInt(0, seconds)) * time.Second)
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

func randYear(min string, max string) string {
	if len(min) == 0 || len(max) == 0 {
		return fmt.Sprintf("%04d", time.Now().Year()-randInt(0, 10))
	}

	minTime, _ := time.Parse(yearFormat, min)
	maxTime, _ := time.Parse(yearFormat, max)
	seconds := int(maxTime.Sub(minTime).Seconds())
	t := minTime.Add(time.Duration(randInt(0, seconds)) * time.Second)
	return fmt.Sprintf("%04d", t.Year())
}
