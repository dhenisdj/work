package work

import (
	"crypto/rand"
	"fmt"
	"io"
)

func makeIdentifier(h string) string {
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s.%x", h, b)
}

func makeSuffix() string {
	b := make([]byte, 4)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}
