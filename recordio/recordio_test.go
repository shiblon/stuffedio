package recordio

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// 63 Characters, since 252 = 63 * 4.
const c63 = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-"

func ExampleEncoder() {
	buf := new(bytes.Buffer)

	enc := NewEncoder(buf)
	if _, err := enc.Encode([]byte("A very short message")); err != nil {
		log.Fatalf("Error appending: %v", err)
	}

	fmt.Printf("%q\n", string(buf.Bytes()))

	// Output:
	// "\xfe\xfd\x14A very short message"
}

func ExampleDecoder() {
	dec := NewDecoder(bytes.NewBuffer([]byte("\xfe\xfd\x14A very short message\x00\x00\xfe\xfd\x05hello")))
	for !dec.Done() {
		b, err := dec.Next()
		if err != nil {
			log.Fatalf("Error reading: %v", err)
		}
		fmt.Printf("%q\n", string(b))
	}

	// Output:
	// "A very short message\xfe\xfd"
	// "hello"
}

func ExampleDecoder_SkipPartial() {
	dec := NewDecoder(bytes.NewBuffer([]byte("middle-of-record\xfe\xfd\x02AB")))
	if err := dec.SkipPartial(); err != nil {
		log.Fatalf("Error skipping partial content: %v", err)
	}
	for !dec.Done() {
		b, err := dec.Next()
		if err != nil {
			log.Fatalf("Error reading: %v", err)
		}
		fmt.Printf("%q\n", string(b))
	}

	// Output:
	// "AB"
}

func TestEncoder_Encode_one(t *testing.T) {
	cases := []struct {
		name  string
		write string
		raw   string
		want  string
	}{
		{
			name:  "simple",
			write: "Short message",
			raw:   "\xfe\xfd\x0dShort message",
		},
		{
			name:  "tail-delimiters",
			write: "short\xfe\xfd",
			raw:   "\xfe\xfd\x05short\x00\x00",
		},
		{
			name:  "leading-delimiters",
			write: "\xfe\xfdshort",
			raw:   "\xfe\xfd\x00\x05\x00short",
		},
		{
			name:  "middle-delimiters",
			write: "short\xfe\xfdmessage",
			raw:   "\xfe\xfd\x05short\x07\x00message",
		},
		{
			name:  "partial-delimiters",
			write: "short\xfemessage",
			raw:   "\xfe\xfd\x0dshort\xfemessage",
		},
		{
			name:  "second-half-delimiters",
			write: "short\xfdmessage",
			raw:   "\xfe\xfd\x0dshort\xfdmessage",
		},
		{
			name:  "empty",
			write: "",
			raw:   "",
		},
		{
			name:  "exact-short",
			write: strings.Repeat(c63, 4), // 252
			raw:   "\xfe\xfd\xfc" + strings.Repeat(c63, 4),
		},
		{
			name:  "exactly-long-one",
			write: strings.Repeat(c63, 4) + strings.Repeat(c63, 1016), // 252 + 64008
			raw:   "\xfe\xfd\xfc" + strings.Repeat(c63, 4) + "\xfc\xfc" + strings.Repeat(c63, 1016),
		},
		{
			name:  "exactly-long-two",
			write: strings.Repeat(c63, 4) + strings.Repeat(c63, 1016) + strings.Repeat(c63, 1016), // 252 + 64008 + 64008
			raw:   "\xfe\xfd\xfc" + strings.Repeat(c63, 4) + "\xfc\xfc" + strings.Repeat(c63, 1016) + "\xfc\xfc" + strings.Repeat(c63, 1016),
		},
		{
			name:  "exact-short-plus-delim",
			write: strings.Repeat(c63, 4) + "\xfe\xfd",
			raw:   "\xfe\xfd\xfc" + strings.Repeat(c63, 4) + "\x00\x00\x00\x00", // explicit delimiter plus zero-length non-delimiter run.
		},
		{
			name:  "exact-long-plus-delim",
			write: strings.Repeat(c63, 4) + strings.Repeat(c63, 1016) + "\xfe\xfd",
			raw:   "\xfe\xfd\xfc" + strings.Repeat(c63, 4) + "\xfc\xfc" + strings.Repeat(c63, 1016) + "\x00\x00\x00\x00",
		},
		{
			name:  "exact-short-ends-with-delim",
			write: strings.Repeat(c63, 4)[:250] + "\xfe\xfd",
			raw:   "\xfe\xfd\xfa" + strings.Repeat(c63, 4)[:250] + "\x00\x00",
		},
		{
			name:  "exact-long-ends-with-delim",
			write: strings.Repeat(c63, 4) + strings.Repeat(c63, 1016)[:64006] + "\xfe\xfd",
			raw:   "\xfe\xfd\xfc" + strings.Repeat(c63, 4) + "\xfa\xfc" + strings.Repeat(c63, 1016)[:64006] + "\x00\x00",
		},
		{
			name:  "longer-message",
			write: "This is a much longer message, containing many more characters than can fit into a single short message of 252 characters. It kind of rambles on, as a result. Good luck figuring out where the break needs to be! It turns out that 252 bytes is really quite a lot of text for a short test like this.",
			raw:   "\xfe\xfd\xfcThis is a much longer message, containing many more characters than can fit into a single short message of 252 characters. It kind of rambles on, as a result. Good luck figuring out where the break needs to be! It turns out that 252 bytes is really qui\x2c\x00te a lot of text for a short test like this.",
		},
		{
			name:  "longer-message-with-delimiter",
			write: "This is a much longer message, containing many more characters than can fit into a single short message of 252 characters. It kind of rambles on, as a result. Good luck figuring out where the break needs to be! It turns out that 252 bytes is really quite a lot of text for a short test like this.\xfe\xfd",
			raw:   "\xfe\xfd\xfcThis is a much longer message, containing many more characters than can fit into a single short message of 252 characters. It kind of rambles on, as a result. Good luck figuring out where the break needs to be! It turns out that 252 bytes is really qui\x2c\x00te a lot of text for a short test like this.\x00\x00",
		},
		{
			name:  "longer-message-with-split-delimiter",
			write: "This is a much longer message, containing many more characters than can fit into a single short message of 252 characters. It kind of rambles on, as a result. Good luck figuring out where the break needs to be! It turns out that 252 bytes is really qu\xfe\xfdite a lot of text for a short test like this.\xfe\xfd",
			raw:   "\xfe\xfd\xfcThis is a much longer message, containing many more characters than can fit into a single short message of 252 characters. It kind of rambles on, as a result. Good luck figuring out where the break needs to be! It turns out that 252 bytes is really qu\xfe\x2e\x00\xfdite a lot of text for a short test like this.\x00\x00",
		},
		{
			name:  "really-long-message",
			write: strings.Repeat("a", 252) + strings.Repeat("b", 64008) + strings.Repeat("c", 1024),
			raw:   "\xfe\xfd\xfc" + strings.Repeat("a", 252) + "\xfc\xfc" + strings.Repeat("b", 64008) + "\x0c\x04" + strings.Repeat("c", 1024),
		},
	}

	for _, test := range cases {
		buf := new(bytes.Buffer)
		enc := NewEncoder(buf)
		if _, err := enc.Encode([]byte(test.write)); err != nil {
			t.Fatalf("Encode_one %q: writing: %v", test.name, err)
		}
		if diff := cmp.Diff(test.raw, string(buf.Bytes())); diff != "" {
			t.Fatalf("Encode_one %q: unexpected diff (-want +got):\n%v", test.name, diff)
		}
		dec := NewDecoder(buf)
		b, err := dec.Next()
		switch {
		case test.raw == "" && !errors.Is(err, io.EOF):
			t.Fatalf("Encode_one %q: expected EOF, got %v with value %q", test.name, err, string(b))
		case test.raw == "" && errors.Is(err, io.EOF):
			// Do nothing
		case err != nil:
			t.Fatalf("Encode_one %q: reading: %v", test.name, err)
		}
		if want, got := test.write, string(b); want != got {
			t.Errorf("Encode_one %q: wanted read %q, got %q", test.name, want, got)
		}
	}
}

func TestEncoder_Encode_multiple(t *testing.T) {
	cases := []struct {
		name  string
		write []string
	}{
		{
			name:  "simple",
			write: []string{"hello", "there", "person"},
		},
		{
			name:  "with delimiters",
			write: []string{"hello\xfe\xfd", "\xfe\xfdthere", "per\xfe\xfdson", "hey", "mul\xfe\xfdtiple\xfe\xfddelimiters"},
		},
	}

	for _, test := range cases {
		buf := new(bytes.Buffer)
		enc := NewEncoder(buf)
		for _, val := range test.write {
			if _, err := enc.Encode([]byte(val)); err != nil {
				t.Fatalf("Encode_multiple %q: %v", test.name, err)
			}
		}

		var got []string
		dec := NewDecoder(buf)
		for !dec.Done() {
			b, err := dec.Next()
			if err != nil {
				t.Fatalf("Encode_multiple %q: %v", test.name, err)
			}
			got = append(got, string(b))
		}

		if diff := cmp.Diff(test.write, got); diff != "" {
			t.Errorf("Encode_multiple: %q unexpected diff (+got -want):\n%v", test.name, diff)
		}
	}
}

func TestDecoder_Next_corrupt(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string // empty means "expect a corruption error" for this test sequence
	}{
		{
			name: "missing-delimiter",
			raw:  "hello",
			want: []string{""},
		},
		{
			name: "garbage-in-front",
			raw:  "hello\xfe\xfd\x01A",
			want: []string{"", "A"},
		},
		{
			name: "garbage-in-back",
			raw:  "\xfe\xfd\x02AB\xfe\xfdfooey",
			want: []string{"AB", ""},
		},
		{
			name: "garbage-in-middle",
			raw:  "\xfe\xfd\x02AB\xfe\xfdrandom crap\xfe\xfd\x02BC\xfe\xfd\x02CD",
			want: []string{"AB", "", "BC", "CD"},
		},
	}

	for _, test := range cases {
		buf := bytes.NewBuffer([]byte(test.raw))
		dec := NewDecoder(buf)
		for i, want := range test.want {
			b, err := dec.Next()
			if err != nil {
				switch {
				case want == "" && errors.Is(err, CorruptRecord):
					// do nothing, this is fine
				case want == "" && !errors.Is(err, CorruptRecord):
					t.Fatalf("Next_corrupt %q i=%d: expected corruption error, got %v with value %q", test.name, i, err, string(b))
				default:
					t.Fatalf("Next_corrupt %q i=%d: %v", test.name, i, err)
				}
			}
			if diff := cmp.Diff(string(b), want); diff != "" {
				t.Errorf("Next_corrupt %q: unexpected diff in record %d:\n%v", test.name, i, diff)
			}
		}
	}
}

func TestDecoder_Consumed(t *testing.T) {
	type entry struct {
		rawLen int64
		val    string
	}

	cases := []struct {
		name    string
		entries []entry
	}{
		{
			name: "basic",
			entries: []entry{
				{8, "Hello"},
				{8, "There"},
			},
		},
		{
			name: "full short",
			entries: []entry{
				{255, strings.Repeat(c63, 4)},
			},
		},
		{
			name: "long-entries",
			entries: []entry{
				{517, strings.Repeat("0123456789abcdef", 32)},
				{16389, strings.Repeat("0123456789abcdef", 1024)},
			},
		},
	}

	for _, test := range cases {
		buf := new(bytes.Buffer)
		enc := NewEncoder(buf)
		for i, e := range test.entries {
			if _, err := enc.Encode([]byte(e.val)); err != nil {
				t.Fatalf("Consumed %q (%d): Error appending: %v", test.name, i, err)
			}
		}

		dec := NewDecoder(buf)
		if dec.Consumed() != 0 {
			t.Fatalf("Consumed %q: Unexpected non-zero consumed value %d", test.name, dec.Consumed())
		}
		totalConsumed := int64(0)
		for i := 0; i < len(test.entries) && !dec.Done(); i++ {
			b, err := dec.Next()
			if err != nil {
				t.Fatalf("Consumed %q (%d): Unexpected error reading: %v", test.name, i, err)
			}
			if diff := cmp.Diff(test.entries[i].val, string(b)); diff != "" {
				//log.Printf("want len %d, got len %d", len(test.entries[i].val), len(b))
				t.Fatalf("Consumed %q (%d): Unexpected diff (-want +got):\n%v", test.name, i, diff)
			}
			if want, got := totalConsumed+test.entries[i].rawLen, dec.Consumed(); want != got {
				t.Fatalf("Consumed %q (%d): Wanted %d consumed, got %d", test.name, i, want, got)
			}
			totalConsumed = dec.Consumed()
		}
	}
}

func TestReverseDecoder(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		errs int
		want []string
	}{
		{
			name: "basic",
			raw:  "\xfe\xfd\x03ABC\xfe\xfd\x03DEF",
			want: []string{"DEF", "ABC"},
		},
		{
			name: "missing-initial",
			raw:  "ABC\xfe\xfd\x03DEF",
			want: []string{"DEF"},
			errs: 1,
		},
		{
			name: "corrupt-end",
			raw:  "\xfe\xfd\x03ABC\xfe\xfd\x03DEFG",
			want: []string{"ABC"},
			errs: 1,
		},
		{
			name: "corrupt-beginning",
			raw:  "\xfe\xfd\x02ABC\xfe\xfd\x03DEF",
			want: []string{"DEF"},
			errs: 1,
		},
		{
			name: "corrupt-affects-neighbor",
			raw:  "\xfe\xfd\x04ABC\xfe\xfd\x03DEF",
			want: []string{"DEF"},
			errs: 1,
		},
		{
			name: "adjacent-delimiters",
			raw:  "\xfe\xfd\xfe\xfd\xfe\xfd",
			want: nil,
			errs: 3,
		},
		{
			name: "one-good-some-adjacent",
			raw:  "\xfe\xfd\x03ABC\xfe\xfd\xfe\xfd",
			want: []string{"ABC"},
			errs: 2,
		},
	}

	for _, test := range cases {
		u := NewReverseDecoder(bytes.NewReader([]byte(test.raw)), int64(len(test.raw)))
		var got []string
		errs := 0
		for !u.Done() {
			b, err := u.Next()
			if err != nil {
				errs++
				continue
			}
			got = append(got, string(b))
		}

		if test.errs != errs {
			t.Fatalf("ReverseDecoder %q: expected %d errors, got %d", test.name, test.errs, errs)
		}

		if diff := cmp.Diff(test.want, got); diff != "" {
			t.Errorf("ReverseDecoder %q: unexpected diff (-want +got):\n%v", test.name, diff)
		}
	}
}

func TestReverseDecoder_LongEntries(t *testing.T) {
	cases := []struct {
		name string
		msgs []string // we always want them in reverse
	}{
		{
			name: "one-long-record",
			msgs: []string{strings.Repeat("a", 100000)},
		},
		{
			name: "two-long-records",
			msgs: []string{
				strings.Repeat("a", 100000),
				strings.Repeat("b", 100000),
			},
		},
		{
			name: "two-long-one-short",
			msgs: []string{
				strings.Repeat("a", 100000),
				strings.Repeat("b", 200),
				strings.Repeat("c", 100000),
			},
		},
	}

	for _, test := range cases {
		buf := new(bytes.Buffer)
		s := NewEncoder(buf)
		for _, msg := range test.msgs {
			if _, err := s.Encode([]byte(msg)); err != nil {
				t.Fatalf("ReverseDecoder long entries %q append: %v", test.name, err)
			}
		}

		u := NewReverseDecoder(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
		var reverseGot []string
		for !u.Done() {
			b, err := u.Next()
			if err != nil {
				t.Fatalf("ReverseDecoder long entries %q next: %v", test.name, err)
			}
			reverseGot = append([]string{string(b)}, reverseGot...)
		}

		if diff := cmp.Diff(test.msgs, reverseGot); diff != "" {
			t.Errorf("ReverseDecoder long entries %q unexpected diff (-want +reverseGot):\n%v", test.name, diff)
		}
	}
}

func ExampleReverseDecoder() {
	msgs := []string{
		"Message 1",
		"Message 2",
		"Message 3",
		"Message 4",
	}

	buf := new(bytes.Buffer)

	s := NewEncoder(buf)
	for _, msg := range msgs {
		if _, err := s.Encode([]byte(msg)); err != nil {
			log.Fatalf("Error appending: %v", err)
		}
	}

	u := NewReverseDecoder(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	for !u.Done() {
		b, err := u.Next()
		if err != nil {
			log.Fatalf("Error reading in reverse: %v", err)
		}
		fmt.Printf("%q\n", b)
	}

	// Output
	// "Message 4"
	// "Message 3"
	// "Message 2"
	// "Message 1"
}
