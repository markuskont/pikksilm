package models

import (
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

var Decoder = jsoniter.ConfigCompatibleWithStandardLibrary
const ArgTimeFormat = "2006-01-02 15:04:05"

// Entry is a nested map with helper methods for recursive lookups
type Entry map[string]interface{}

func (d Entry) GetDot(key string) (interface{}, bool) {
	bits := strings.Split(key, ".")
	if len(bits) == 0 {
		return nil, false
	}
	return d.Get(bits...)
}

// Get is a helper for doing recursive lookups into nested maps (nested JSON). Key argument is a
// slice of strings
func (d Entry) Get(key ...string) (interface{}, bool) {
	if len(key) == 0 {
		return nil, false
	}
	if val, ok := d[key[0]]; ok {
		switch res := val.(type) {
		case map[string]interface{}:
			// key has only one item, user wants the map itselt, not subelement
			if len(key) == 1 {
				return res, ok
			}
			// recurse with key remainder
			return Entry(res).Get(key[1:]...)
		default:
			return val, ok
		}
	}
	return nil, false
}

// GetString wraps Get to cast item to string. Returns 3-tuple with value, key presence and
// correct type assertion respectively.
func (d Entry) GetString(key string) (string, bool) {
	if val, ok := d.GetDot(key); ok {
		str, ok := val.(string)
		if !ok {
			return "", false
		}
		return str, true
	}
	return "", false
}

func (d Entry) GetTimestamp(key string) (time.Time, bool, error) {
	val, ok := d.GetString(key)
	if !ok {
		return time.Time{}, false, nil
	}
	ts, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return time.Time{}, false, err
	}
	return ts, true, nil
}
