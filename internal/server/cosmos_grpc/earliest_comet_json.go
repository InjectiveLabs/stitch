package cosmos_grpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	cometResponseBudget   = 128 * 1024 * 1024
	cometProjectionBudget = 64 * 1024
	cometJSONDepth        = 64
)

// A block/results response can contain many MiB of transactions and events.
// Discovery needs only height, error details, and proof presence. A normal
// json.Decoder.Decode into a small struct still buffers the entire JSON value;
// Token also allocates complete transaction strings. This projection validates
// and consumes every byte while discarding unselected strings as they stream.
type cometJSONRule struct {
	fields         map[string]*cometJSONRule
	keep           bool
	presence       bool
	element        *cometJSONRule
	expected       byte
	required       []string
	stringPresence bool
	nonempty       bool
}

var cometScalarRule = &cometJSONRule{keep: true}
var cometProofOpRule = &cometJSONRule{
	expected: '{',
	required: []string{"type", "data"},
	fields: map[string]*cometJSONRule{
		"type": {expected: '"', nonempty: true},
		"data": {expected: '"', nonempty: true, stringPresence: true},
	},
}
var cometProofRule = &cometJSONRule{fields: map[string]*cometJSONRule{
	"ops": {presence: true, element: cometProofOpRule},
}}
var cometEnvelopeRule = &cometJSONRule{fields: map[string]*cometJSONRule{
	"result": {fields: map[string]*cometJSONRule{
		"height": cometScalarRule,
		"block": {fields: map[string]*cometJSONRule{
			"header": {fields: map[string]*cometJSONRule{"height": cometScalarRule}},
		}},
		"response": {fields: map[string]*cometJSONRule{
			"code": cometScalarRule, "log": cometScalarRule, "height": cometScalarRule,
			"proofOps": cometProofRule, "proof_ops": cometProofRule,
		}},
	}},
	"error": {fields: map[string]*cometJSONRule{
		"code": cometScalarRule, "message": cometScalarRule, "data": cometScalarRule,
	}},
}}

type cometJSONProjection struct{ reader *bufio.Reader }

func projectCometResponse(body io.Reader) ([]byte, error) {
	limit := &io.LimitedReader{R: body, N: cometResponseBudget + 1}
	p := cometJSONProjection{reader: bufio.NewReaderSize(limit, 32*1024)}
	result, err := p.value(cometEnvelopeRule, 0)
	if err == nil {
		var next byte
		next, err = p.next()
		switch err {
		case io.EOF:
			err = nil
		case nil:
			err = fmt.Errorf("unexpected trailing JSON byte %q", next)
		}
	}
	if limit.N <= 0 {
		return nil, errors.New("comet capability response exceeds 128 MiB")
	}
	return result, err
}

func (p *cometJSONProjection) next() (byte, error) {
	for {
		b, err := p.reader.ReadByte()
		if err != nil {
			return 0, err
		}
		switch b {
		case ' ', '\t', '\r', '\n':
		default:
			return b, nil
		}
	}
}

func (p *cometJSONProjection) value(rule *cometJSONRule, depth int) ([]byte, error) {
	if depth > cometJSONDepth {
		return nil, errors.New("comet JSON nesting exceeds 64 levels")
	}
	b, err := p.next()
	if err != nil {
		return nil, err
	}
	if rule != nil && rule.expected != 0 && b != rule.expected {
		return nil, errors.New("invalid proof operation field type")
	}
	switch b {
	case '{':
		return p.object(rule, depth+1)
	case '[':
		return p.array(rule, depth+1)
	case '"':
		if rule != nil && rule.stringPresence {
			if _, err := p.stringValue(false, rule.nonempty); err != nil {
				return nil, err
			}
			return []byte(`"present"`), nil
		}
		return p.stringValue(rule != nil, rule != nil && rule.nonempty)
	default:
		// Numbers/literals are bounded independently of retained output.
		// Unlike strings, JSON scalar numbers in Comet are always small.
		data := []byte{b}
		for {
			next, err := p.reader.ReadByte()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			if next == ',' || next == '}' || next == ']' || next == ' ' || next == '\t' || next == '\r' || next == '\n' {
				_ = p.reader.UnreadByte()
				break
			}
			if len(data) >= 1024 {
				return nil, errors.New("comet JSON scalar exceeds 1024 bytes")
			}
			data = append(data, next)
		}
		if !json.Valid(data) || (data[0] != '-' && (data[0] < '0' || data[0] > '9') && string(data) != "true" && string(data) != "false" && string(data) != "null") {
			return nil, errors.New("invalid Comet JSON scalar")
		}
		if rule != nil {
			return data, nil
		}
		return nil, nil
	}
}

func (p *cometJSONProjection) object(rule *cometJSONRule, depth int) ([]byte, error) {
	var result []byte
	var seen map[string]bool
	if rule != nil {
		result = append(result, '{')
		if len(rule.required) != 0 {
			seen = make(map[string]bool, len(rule.required))
		}
	}
	finish := func() ([]byte, error) {
		if rule != nil {
			for _, field := range rule.required {
				if !seen[field] {
					return nil, fmt.Errorf("proof operation lacks %s", field)
				}
			}
			result = append(result, '}')
		}
		return result, nil
	}
	first := true
	for {
		b, err := p.next()
		if err != nil {
			return nil, err
		}
		if b == '}' && first {
			return finish()
		}
		if b != '"' {
			return nil, errors.New("comet JSON object requires a field name")
		}
		// Unknown subtrees do not even need to retain their field names.
		key, err := p.stringValue(rule != nil, false)
		if err != nil {
			return nil, err
		}
		if b, err = p.next(); err != nil || b != ':' {
			return nil, errors.New("comet JSON object requires a colon")
		}
		var child *cometJSONRule
		if rule != nil {
			if rule.keep {
				child = rule
			} else {
				var name string
				if err := json.Unmarshal(key, &name); err != nil {
					return nil, err
				}
				child = rule.fields[name]
				if seen != nil && child != nil {
					seen[name] = true
				}
			}
		}
		value, err := p.value(child, depth)
		if err != nil {
			return nil, err
		}
		if child != nil {
			if len(result) > 1 {
				result = append(result, ',')
			}
			if len(result)+len(key)+len(value)+2 > cometProjectionBudget {
				return nil, errors.New("comet discovery metadata exceeds 64 KiB")
			}
			result = append(result, key...)
			result = append(result, ':')
			result = append(result, value...)
		}
		first = false
		b, err = p.next()
		if err != nil {
			return nil, err
		}
		if b == '}' {
			return finish()
		}
		if b != ',' {
			return nil, errors.New("comet JSON object requires a comma")
		}
	}
}

func (p *cometJSONProjection) array(rule *cometJSONRule, depth int) ([]byte, error) {
	var result []byte
	if rule != nil {
		result = append(result, '[')
	}
	first := true
	for {
		b, err := p.next()
		if err != nil {
			return nil, err
		}
		if b == ']' && first {
			if rule != nil {
				result = append(result, ']')
			}
			return result, nil
		}
		_ = p.reader.UnreadByte()
		var child *cometJSONRule
		if rule != nil && rule.keep {
			child = rule
		} else if rule != nil && rule.presence {
			child = rule.element
		}
		value, err := p.value(child, depth)
		if err != nil {
			return nil, err
		}
		if child != nil && (rule == nil || !rule.presence) {
			if !first {
				result = append(result, ',')
			}
			if len(result)+len(value)+1 > cometProjectionBudget {
				return nil, errors.New("comet discovery metadata exceeds 64 KiB")
			}
			result = append(result, value...)
		} else if rule != nil && rule.presence && first {
			// Validate every operation, retaining only the first witness.
			// Its data was reduced to a nonempty marker during streaming.
			result = append(result, value...)
		}
		first = false
		b, err = p.next()
		if err != nil {
			return nil, err
		}
		if b == ']' {
			if rule != nil {
				result = append(result, ']')
			}
			return result, nil
		}
		if b != ',' {
			return nil, errors.New("comet JSON array requires a comma")
		}
	}
}

// stringValue starts immediately after the opening quote. It validates escape
// syntax without allocating skipped transaction/event strings, including a
// single string much larger than the retained metadata budget.
func (p *cometJSONProjection) stringValue(keep, nonempty bool) ([]byte, error) {
	var result []byte
	hasContent := false
	if keep {
		result = append(result, '"')
	}
	appendByte := func(b byte) error {
		if !keep {
			return nil
		}
		if len(result) >= cometProjectionBudget {
			return errors.New("comet discovery metadata string exceeds 64 KiB")
		}
		result = append(result, b)
		return nil
	}
	for {
		b, err := p.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if b < 0x20 {
			return nil, errors.New("unescaped control byte in Comet JSON string")
		}
		if err := appendByte(b); err != nil {
			return nil, err
		}
		if b == '"' {
			if nonempty && !hasContent {
				return nil, errors.New("proof operation requires a nonempty string")
			}
			return result, nil
		}
		hasContent = true
		if b != '\\' {
			continue
		}
		escaped, err := p.reader.ReadByte()
		if err != nil {
			return nil, err
		}
		if err := appendByte(escaped); err != nil {
			return nil, err
		}
		switch escaped {
		case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
		case 'u':
			for i := 0; i < 4; i++ {
				h, err := p.reader.ReadByte()
				if err != nil {
					return nil, err
				}
				if (h < '0' || h > '9') && (h < 'a' || h > 'f') && (h < 'A' || h > 'F') {
					return nil, errors.New("invalid Unicode escape in Comet JSON")
				}
				if err := appendByte(h); err != nil {
					return nil, err
				}
			}
		default:
			return nil, errors.New("invalid escape in Comet JSON string")
		}
	}
}
