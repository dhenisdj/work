package utils

import (
	"crypto/rand"
	"fmt"
	"io"
)

func MakeIdentifier() string {
	b := make([]byte, 6)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

func MakeSuffix() string {
	b := make([]byte, 4)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}
