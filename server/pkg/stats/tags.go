package stats

import "strconv"

// Tag placement constants
const (
	TagPlacementName = iota
	TagPlacementSuffix
)

// TagFormat controls tag formatting style
type TagFormat struct {
	// FirstSeparator is put after metric name and before first tag
	FirstSeparator string
	// Placement specifies part of the message to append tags to
	Placement byte
	// OtherSeparator separates 2nd and subsequent tags from each other
	OtherSeparator byte
	// KeyValueSeparator separates tag name and tag value
	KeyValueSeparator byte
}

// Tag types
const (
	typeString = iota
	typeInt64
)

// Tag is metric-specific tag
type Tag struct {
	name     string
	strvalue string
	intvalue int64
	typ      byte
}

// Append formats tag and appends it to the buffer
func (tag Tag) Append(buf []byte, style *TagFormat) []byte {
	buf = append(buf, []byte(tag.name)...)
	buf = append(buf, style.KeyValueSeparator)
	if tag.typ == typeString {
		return append(buf, []byte(tag.strvalue)...)
	}
	return strconv.AppendInt(buf, tag.intvalue, 10)
}

// StringTag creates Tag with string value
func StringTag(name, value string) Tag {
	return Tag{name: name, strvalue: value, typ: typeString}
}

// IntTag creates Tag with integer value
func IntTag(name string, value int) Tag {
	return Tag{name: name, intvalue: int64(value), typ: typeInt64}
}

// Int64Tag creates Tag with integer value
func Int64Tag(name string, value int64) Tag {
	return Tag{name: name, intvalue: value, typ: typeInt64}
}

func (s *Stats) formatTags(buf []byte, tags []Tag) []byte {
	tagsLen := len(s.defaultTags) + len(tags)
	if tagsLen == 0 {
		return buf
	}

	buf = append(buf, []byte(s.trans.tagFormat.FirstSeparator)...)
	for i := range s.defaultTags {
		buf = s.defaultTags[i].Append(buf, s.trans.tagFormat)
		if i != tagsLen-1 {
			buf = append(buf, s.trans.tagFormat.OtherSeparator)
		}
	}

	for i := range tags {
		buf = tags[i].Append(buf, s.trans.tagFormat)
		if i+len(s.defaultTags) != tagsLen-1 {
			buf = append(buf, s.trans.tagFormat.OtherSeparator)
		}
	}

	return buf
}

var (
	// TagFormatInfluxDB is format for InfluxDB StatsD telegraf plugin
	//
	// Docs: https://github.com/influxdata/telegraf/tree/master/plugins/inputs/statsd
	TagFormatInfluxDB = &TagFormat{
		Placement:         TagPlacementName,
		FirstSeparator:    ",",
		OtherSeparator:    ',',
		KeyValueSeparator: '=',
	}

	// TagFormatDatadog is format for DogStatsD (Datadog Agent)
	//
	// Docs: https://docs.datadoghq.com/developers/dogstatsd/#datagram-format
	TagFormatDatadog = &TagFormat{
		Placement:         TagPlacementSuffix,
		FirstSeparator:    "|#",
		OtherSeparator:    ',',
		KeyValueSeparator: ':',
	}
)
