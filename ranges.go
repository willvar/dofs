package dofs

type byteRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

func addByteRange(ranges []byteRange, start, end int64) []byteRange {
	if start >= end {
		return ranges
	}
	merged := make([]byteRange, 0, len(ranges)+1)
	candidate := byteRange{Start: start, End: end}
	inserted := false
	for _, current := range ranges {
		if current.End < candidate.Start {
			merged = append(merged, current)
			continue
		}
		if candidate.End < current.Start {
			if !inserted {
				merged = append(merged, candidate)
				inserted = true
			}
			merged = append(merged, current)
			continue
		}
		candidate.Start = min(candidate.Start, current.Start)
		candidate.End = max(candidate.End, current.End)
	}
	if !inserted {
		merged = append(merged, candidate)
	}
	return merged
}

func missingByteRanges(ranges []byteRange, start, end int64) []byteRange {
	if start >= end {
		return nil
	}
	cursor := start
	var missing []byteRange
	for _, current := range ranges {
		if current.End <= cursor {
			continue
		}
		if current.Start >= end {
			break
		}
		if current.Start > cursor {
			missing = append(missing, byteRange{Start: cursor, End: min(current.Start, end)})
		}
		cursor = max(cursor, current.End)
		if cursor >= end {
			return missing
		}
	}
	if cursor < end {
		missing = append(missing, byteRange{Start: cursor, End: end})
	}
	return missing
}
