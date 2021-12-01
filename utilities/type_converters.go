/**
 * Copyright (c) 2020-present, The cloudquery authors
 *
 * This source code is licensed as defined by the LICENSE file found in the
 * root directory of this source tree.
 *
 * SPDX-License-Identifier: (Apache-2.0 OR GPL-2.0-only)
 */

package utilities

import (
	"encoding/json"
	"strconv"
)

// GetStringValue returns the string representation of given interface
func GetStringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	switch value := value.(type) {
	case string:
		strIn := []rune(value)
		if len(strIn) >= 2 && strIn[0] == '"' && strIn[len(strIn)-1] == '"' {
			// Remove quotes
			noLeadingQuotes := strIn[1:]
			return string(noLeadingQuotes[:len(noLeadingQuotes)-1])
		}
		return value
	case json.Number:
		val, _ := value.Int64()
		return strconv.FormatInt(val, 10)
	case bool:
		return strconv.FormatBool(value)
	}

	return getNumericStringValue(value)
}

func getNumericStringValue(value interface{}) string {
	if value == nil {
		return ""
	}
	switch v := value.(type) {
	case int:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case int8:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case int16:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case int32:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case int64:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case uint:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case uint8:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case uint16:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case uint32:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case uint64:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(v), 'g', -1, 64)
	case float64:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}

	return ""
}
