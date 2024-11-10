package encoding

import (
	"bytes"
	"encoding/binary"

	BTree "github.com/yash7xm/RelixDB/BTree"
	Table "github.com/yash7xm/RelixDB/Relational"
)

func EncodeValues(out []byte, vals []Table.Value) []byte {
	for _, v := range vals {
		switch v.Type {
		case Table.TYPE_INT64:
			var buf [8]byte
			u := uint64(v.I64) + (1 << 63)
			binary.BigEndian.PutUint64(buf[:], u)
			out = append(out, buf[:]...)
		case Table.TYPE_BYTES:
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

func DecodeValues(in []byte, out []Table.Value) []Table.Value {
	str, _ := unescapeString(in[:])
	out = append(out, Table.Value{Type: Table.TYPE_BYTES, Str: []byte(str)})
	return out
}

// unescapeString reverses the escapeString process
func unescapeString(in []byte) (string, int) {
	out := make([]byte, 0, len(in))
	i := 0
	for i < len(in) {
		if in[i] == 0x01 {
			if in[i+1] == 0x01 {
				out = append(out, 0) // "\x01\x01" -> "\x00"
			} else if in[i+1] == 0x02 {
				out = append(out, 0x01) // "\x01\x02" -> "\x01"
			} else {
				panic("invalid escape sequence")
			}
			i += 2 // Move past the escape sequence
		} else if in[i] == 0 {
			break // Null-terminator found, end of string
		} else {
			out = append(out, in[i])
			i++
		}
	}
	return string(out), i
}

// for primary keys
func EncodeKey(out []byte, prefix uint32, vals []Table.Value) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], prefix)
	out = append(out, buf[:]...)
	out = EncodeValues(out, vals)
	return out
}

// The range key can be a prefix of the index key,
// we may have to encode missing columns to make the comparison work.
func EncodeKeyPartial(
	out []byte, prefix uint32, values []Table.Value,
	tdef *Table.TableDef, keys []string, cmp int,
) []byte {
	out = EncodeKey(out, prefix, values)
	// Encode the missing columns as either minimum or maximum values,
	// depending on the comparison operator.
	// 1. The empty string is lower than all possible value encodings,
	// thus we don't need to add anything for CMP_LT and CMP_GE.
	// 2. The maximum encodings are all 0xff bytes.
	max := cmp == BTree.CMP_GT || cmp == BTree.CMP_LE
loop:
	for i := len(values); max && i < len(keys); i++ {
		switch tdef.Types[Table.ColIndex(tdef, keys[i])] {
		case Table.TYPE_BYTES:
			out = append(out, 0xff)
			break loop // stops here since no string encoding starts with 0xff
		case Table.TYPE_INT64:
			out = append(out, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
		default:
			panic("what?")
		}
	}
	return out
}
