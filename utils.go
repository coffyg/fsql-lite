// utils.go - Utility functions for fsql-lite
package fsql

import (
	"fmt"
	"strconv"
	"strings"
)

// IntListToStrComma converts a slice of ints to a comma-separated string
func IntListToStrComma(list []int) string {
	if len(list) == 0 {
		return ""
	}
	strs := make([]string, len(list))
	for i, v := range list {
		strs[i] = strconv.Itoa(v)
	}
	return strings.Join(strs, ",")
}

// Int64ListToStrComma converts a slice of int64s to a comma-separated string
func Int64ListToStrComma(list []int64) string {
	if len(list) == 0 {
		return ""
	}
	strs := make([]string, len(list))
	for i, v := range list {
		strs[i] = strconv.FormatInt(v, 10)
	}
	return strings.Join(strs, ",")
}

// StrListToStrComma converts a slice of strings to a comma-separated string
func StrListToStrComma(list []string) string {
	if len(list) == 0 {
		return ""
	}
	return strings.Join(list, ",")
}

// Placeholders generates a slice of PostgreSQL parameter placeholders
func Placeholders(start, count int) []string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf("$%d", start+i)
	}
	return placeholders
}

// PlaceholdersString generates a comma-separated string of PostgreSQL parameter placeholders
func PlaceholdersString(start, count int) string {
	if count == 0 {
		return ""
	}
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf("$%d", start+i)
	}
	return strings.Join(placeholders, ", ")
}
