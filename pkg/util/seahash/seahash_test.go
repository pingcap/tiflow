// The file is copied from blainsmith.com/go/seahash@v1.2.1.

package seahash

import (
	"fmt"
	"math/rand"
	"testing"
)

func ExampleSum() {
	// hash some bytes
	hash := Sum([]byte("to be or not to be"))
	fmt.Printf("%x", hash)
	// Output: 75e54a6f823a991b
}

func ExampleNew() {
	// hash some bytes
	h := New()
	h.Write([]byte("to be or not to be"))
	hash := h.Sum64()
	fmt.Printf("%x", hash)
	// Output: 1b993a826f4ae575
}

func ExampleSum64() {
	// hash some bytes
	fmt.Printf("%x", Sum64([]byte("to be or not to be")))
	// Output: 1b993a826f4ae575
}

func TestRandom(t *testing.T) {
	expected := "d30e85ff891306b8"
	str := []byte("abcdegfhijklmnabcdegfhijklmnabcdegfhijklmn")
	for i := 0; i < 1000; i++ {
		r := rand.NewSource(int64(i))
		s := str
		h := New()
		// Split "str" into random fragments and add them to Write.  The
		// final result should be the same.
		for len(s) > 0 {
			n := int(r.Int63()%int64(len(s))) + 1
			h.Write(s[:n])
			// Make a dummy call to Sum64() to detect when it causes
			// an unwanted state change.
			h.Sum64()
			s = s[n:]
		}
		if s := fmt.Sprintf("%x", h.Sum(nil)); s != expected {
			t.Errorf("seed %d: %v", i, s)
		}
	}
}

func BenchmarkSum(b *testing.B) {
	h := New()
	data := []byte("to be or not to be")
	for i := 0; i < b.N; i++ {
		h.Sum(data)
		h.Reset()
	}
}

func TestSizes(t *testing.T) {
	h := New()

	if h.Size() != 8 {
		t.Fail()
	}

	if h.BlockSize() != 8 {
		t.Fail()
	}
}
