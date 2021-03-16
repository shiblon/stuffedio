package stuffedio

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

func Example() {
	buf := new(bytes.Buffer)

	msgs := []string{
		"A short message",
		"A somewhat longer message",
		"A message with delimiter \xfe\xfd in it",
	}

	// Write some things to the log.
	w := NewWriter(buf)
	for _, msg := range msgs {
		if err := w.Append([]byte(msg)); err != nil {
			log.Fatalf("Error appending: %v", err)
		}
	}

	// Read the records from the log.
	r := NewReader(buf)
	for !r.Done() {
		b, err := r.Next()
		if err != nil {
			if errors.Is(err, CorruptRecord) {
				log.Printf("Error reading record, will skip: %v", err)
				continue
			}
			log.Fatalf("Error reading: %v", err)
		}
		fmt.Printf("%q\n", string(b))
	}

	// Output:
	// "A short message"
	// "A somewhat longer message"
	// "A message with delimiter \xfe\xfd in it"
}

func ExampleWriter() {
	buf := new(bytes.Buffer)

	w := NewWriter(buf)
	if err := w.Append([]byte("A very short message")); err != nil {
		log.Fatalf("Error appending: %v", err)
	}

	fmt.Printf("%q\n", string(buf.Bytes()))

	// Output:
	// "\xfe\xfd\x14A very short message"
}

func ExampleReader() {
	r := NewReader(bytes.NewBuffer([]byte("\xfe\xfd\x14A very short message\x00\x00\xfe\xfd\x05hello")))
	for !r.Done() {
		b, err := r.Next()
		if err != nil {
			log.Fatalf("Error reading: %v", err)
		}
		fmt.Printf("%q\n", string(b))
	}

	// Output:
	// "A very short message\xfe\xfd"
	// "hello"
}

func ExampleReader_SkipPartial() {
	r := NewReader(bytes.NewBuffer([]byte("middle-of-record\xfe\xfd\x02AB")))
	if err := r.SkipPartial(); err != nil {
		log.Fatalf("Error skipping partial content: %v", err)
	}
	for !r.Done() {
		b, err := r.Next()
		if err != nil {
			log.Fatalf("Error reading: %v", err)
		}
		fmt.Printf("%q\n", string(b))
	}

	// Output:
	// "AB"
}

func TestWriter_Append_one(t *testing.T) {
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
			raw:   "\xfe\xfd\xfc" + strings.Repeat(c63, 4) + "\x00\x00\x00\x00", // explicit *and* implicit delimiters added.
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
		w := NewWriter(buf)
		if err := w.Append([]byte(test.write)); err != nil {
			t.Fatalf("Append_one %q: writing: %v", test.name, err)
		}
		if diff := cmp.Diff(test.raw, string(buf.Bytes())); diff != "" {
			t.Fatalf("Append_one %q: unexpected diff (-want +got):\n%v", test.name, diff)
		}
		r := NewReader(buf)
		b, err := r.Next()
		switch {
		case test.raw == "" && !errors.Is(err, io.EOF):
			t.Fatalf("Append_one %q: expected EOF, got %v with value %q", test.name, err, string(b))
		case test.raw == "" && errors.Is(err, io.EOF):
			// Do nothing
		case err != nil:
			t.Fatalf("Append_one %q: reading: %v", test.name, err)
		}
		if want, got := test.write, string(b); want != got {
			t.Errorf("Append_one %q: wanted read %q, got %q", test.name, want, got)
		}
	}
}

func TestWriter_Append_multiple(t *testing.T) {
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
		w := NewWriter(buf)
		for _, val := range test.write {
			if err := w.Append([]byte(val)); err != nil {
				t.Fatalf("Append_multiple %q: %v", test.name, err)
			}
		}

		var got []string
		r := NewReader(buf)
		for !r.Done() {
			b, err := r.Next()
			if err != nil {
				t.Fatalf("Append_multiple %q: %v", test.name, err)
			}
			got = append(got, string(b))
		}

		if diff := cmp.Diff(test.write, got); diff != "" {
			t.Errorf("Append_multiple: %q unexpected diff (+got -want):\n%v", test.name, diff)
		}
	}
}

func TestReader_Next_corrupt(t *testing.T) {
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
		r := NewReader(buf)
		for i, want := range test.want {
			b, err := r.Next()
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

func TestReader_Consumed(t *testing.T) {
	type entry struct {
		rawLen int
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
				{517, strings.Repeat("0123456789abcdefg", 32)},
				{16389, strings.Repeat("0123456789abcdefg", 1024)},
			},
		},
	}

	for _, test := range cases {
		buf := new(bytes.Buffer)
		w := NewWriter(buf)
		for i, e := range test.entries {
			//log.Printf("%q (%d): len=%d", test.name, i, len(e.val))
			if err := w.Append([]byte(e.val)); err != nil {
				t.Fatalf("Consumed %q (%d): Error appending: %v", test.name, i, err)
			}
			//log.Printf("%q", string(buf.Bytes()))
		}

		r := NewReader(buf)
		if r.Consumed() != 0 {
			t.Fatalf("Consumed %q: Unexpected non-zero consumed value %d", test.name, r.Consumed())
		}
		totalConsumed := 0
		for i := 0; i < len(test.entries) && !r.Done(); i++ {
			b, err := r.Next()
			if err != nil {
				t.Fatalf("Consumed %q (%d): Unexpected error reading: %v", test.name, i, err)
			}
			//log.Printf("%q", string(b))
			//log.Printf("test len %d", len(b))
			if diff := cmp.Diff(test.entries[i].val, string(b)); diff != "" {
				//log.Printf("want len %d, got len %d", len(test.entries[i].val), len(b))
				t.Fatalf("Consumed %q (%d): Unexpected diff (-want +got):\n%v", test.name, i, diff)
			}
			if want, got := totalConsumed+test.entries[i].rawLen, r.Consumed(); want != got {
				t.Fatalf("Consumed %q (%d): Wanted %d consumed, got %d", test.name, i, want, got)
			}
			totalConsumed = r.Consumed()
		}
	}
}
