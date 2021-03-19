package stuffedio

import (
	"bytes"
	"strings"
	"testing"
)

func TestMultiUnstuffer_Consumed(t *testing.T) {
	cases := []struct {
		name string
		msgs []string
	}{
		{
			name: "single",
			msgs: []string{strings.Repeat("a", 1000)},
		},
		{
			name: "two",
			msgs: []string{
				strings.Repeat("b", 1000),
				strings.Repeat("c", 1000),
			},
		},
		{
			name: "three",
			msgs: []string{
				strings.Repeat("a", 252),
				strings.Repeat("b", 60008),
				strings.Repeat("c", 1000),
			},
		},
	}

	for _, test := range cases {
		// Place one messae into each unstuffer, then multi-bundle them.
		var components []*Unstuffer
		for _, msg := range test.msgs {
			buf := new(bytes.Buffer)
			s := NewStuffer(buf)
			if err := s.Append([]byte(msg)); err != nil {
				t.Fatalf("%q: error appending: %v", test.name, err)
			}
			components = append(components, NewUnstuffer(buf))
		}

		// Consume everything from a multi unstuffer.
		m := NewMultiUnstuffer(components)
		for !m.Done() {
			if _, err := m.Next(); err != nil {
				t.Fatalf("%q: failed next: %v", test.name, err)
			}
		}

		expected := int64(0)
		for _, u := range components {
			expected += u.Consumed()
		}

		if m.Consumed() != expected {
			t.Errorf("%q: expected %d consumed, got %d", test.name, expected, m.Consumed())
		}
	}
}
