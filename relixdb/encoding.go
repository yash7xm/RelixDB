package relixdb

import (
	"bytes"
	"encoding/binary"
)

func encodeValues(out []byte, vals []Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case TYPE_BYTES:
			out = append(out, escapeString(v.Str)...)
			out = append(out, 0) // null-terminated
		default:
			panic("what?")
		}
	}
	return out
}

// 1. strings are encoded as null-terminated strings,
// escape the null byte so that strings contain no null byte.
// 2. "\xff" represents the highest order in key comparisons,
// also escape the first byte if it's 0xff.
func escapeString(in []byte) []byte {
	zeros := bytes.Count(in, []byte{0})
	ones := bytes.Count(in, []byte{1})
	if zeros+ones == 0 {
		return in
	}
	out := make([]byte, len(in)+zeros+ones)
	pos := 0
	if len(in) > 0 && in[0] >= 0xfe {
		out[0] = 0xfe
		out[1] = in[0]
		pos += 2
		in = in[1:]
	}
	for _, ch := range in {
		if ch <= 1 {
			out[pos+0] = 0x01
			out[pos+1] = ch + 1
			pos += 2
		} else {
			out[pos] = ch
			pos += 1
		}
	}
	return out
}

// Decode the encoded values back to their original form
func decodeValues(in []byte, out []Value) []Value {
	pos := 0
	for pos < len(in) {
		// We need at least one byte to determine the type
		if len(out) == 0 {
			break
		}

		currentValue := &out[0]
		out = out[1:]

		switch currentValue.Type {
		case TYPE_INT64:
			// For int64, we read exactly 8 bytes
			if pos+8 > len(in) {
				panic("incomplete int64 value")
			}
			// Read the uint64 and convert back to int64
			u := binary.BigEndian.Uint64(in[pos : pos+8])
			currentValue.I64 = int64(u - (1 << 63)) // Reverse the sign bit flip
			pos += 8

		case TYPE_BYTES:
			// Find the null terminator and unescape the string
			start := pos
			for pos < len(in) && in[pos] != 0 {
				if in[pos] == 0x01 {
					pos++ // Skip escape byte
					if pos >= len(in) {
						panic("incomplete escape sequence")
					}
				}
				pos++
			}
			if pos >= len(in) {
				panic("unterminated string")
			}

			// Unescape the string
			currentValue.Str = unescapeString(in[start:pos])
			pos++ // Skip null terminator

		default:
			panic("unknown type")
		}
	}
	return out
}

// Unescape the string by converting \x01\x01 back to \x00 and \x01\x02 back to \x01
func unescapeString(in []byte) []byte {
	// Count how many escape sequences we have to allocate the right size
	escapeCount := 0
	for i := 0; i < len(in); i++ {
		if in[i] == 0x01 {
			escapeCount++
			i++ // Skip the next byte as it's part of the escape sequence
		}
	}

	// If no escapes, return the original
	if escapeCount == 0 {
		return in
	}

	// Create the output buffer with the correct size
	out := make([]byte, len(in)-escapeCount)
	pos := 0

	for i := 0; i < len(in); i++ {
		if in[i] == 0x01 {
			i++ // Move to the next byte
			if i >= len(in) {
				panic("incomplete escape sequence")
			}
			out[pos] = in[i] - 1 // Convert back from escaped value
		} else {
			out[pos] = in[i]
		}
		pos++
	}

	return out
}

// for primary keys
func encodeKey(out []byte, prefix uint32, vals []Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = encodeValues(out, vals)
	return out
}

// The range key can be a prefix of the index key,
// we may have to encode missing columns to make the comparison work.
func encodeKeyPartial(
	out []byte, prefix uint32, values []Value,
	tdef *TableDef, keys []string, cmp int,
) []byte {
	out = encodeKey(out, prefix, values)
	// Encode the missing columns as either minimum or maximum values,
	// depending on the comparison operator.
	// 1. The empty string is lower than all possible value encodings,
	// thus we don't need to add anything for CMP_LT and CMP_GE.
	// 2. The maximum encodings are all 0xff bytes.
	max := cmp == CMP_GT || cmp == CMP_LE
loop:
	for i := len(values); max && i < len(keys); i++ {
		switch tdef.Types[colIndex(tdef, keys[i])] {
		case TYPE_BYTES:
			out = append(out, 0xff)
			break loop // stops here since no string encoding starts with 0xff
		case TYPE_INT64:
			out = append(out, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
		default:
			panic("what?")
		}
	}
	return out
}
