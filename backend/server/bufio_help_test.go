package server

import (
	"bufio"
	"io"
)

func newBufioReader(r io.Reader) *bufio.Reader { return bufio.NewReader(r) }
