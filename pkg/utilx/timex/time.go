package timex

import (
	"strconv"
	"time"

	"github.com/pkg/errors"
)

// ParseTimeWithMultipleLayouts parses the time string with the provided layouts or as a numeric timestamp.
func ParseTimeWithMultipleLayouts(s string, layouts ...string) (time.Time, error) {
	// First, try to parse the string as a numeric timestamp
	if timestamp, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.Unix(0, timestamp*int64(time.Millisecond)).UTC(), nil
	}

	// If parsing as a timestamp fails, try the provided layouts
	var (
		parsedTime   time.Time
		err          error
		errParseTime error
	)

	for _, layout := range layouts {
		parsedTime, err = time.Parse(layout, s)
		if err == nil {
			return parsedTime.UTC(), nil
		}

		errParseTime = errors.WithMessagef(err, "unable to parse time string with provided layouts: %s", err.Error())
	}

	return time.Time{}, errParseTime
}
