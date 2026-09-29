package server

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
)

// Minimal RFC 6455 server-side WebSocket using only the standard
// library. Traffic in this lab is tiny, so no permessage-deflate etc.

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

func wsAccept(key string) string {
	h := sha1.New()
	h.Write([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

type wsConn struct {
	conn net.Conn
}

func upgrade(w http.ResponseWriter, r *http.Request) (*wsConn, error) {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") ||
		!strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		return nil, errors.New("not a websocket handshake")
	}
	key := r.Header.Get("Sec-WebSocket-Key")
	hj, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("hijack unsupported")
	}
	conn, buf, err := hj.Hijack()
	if err != nil {
		return nil, err
	}
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + wsAccept(key) + "\r\n\r\n"
	if _, err := buf.WriteString(resp); err != nil {
		return nil, err
	}
	buf.Flush()
	return &wsConn{conn: conn}, nil
}

// readMessage returns one decoded frame payload (fragments joined).
func (c *wsConn) readMessage() ([]byte, error) {
	var payload []byte
	for {
		hdr := make([]byte, 2)
		if _, err := io.ReadFull(c.conn, hdr); err != nil {
			return nil, err
		}
		fin := hdr[0]&0x80 != 0
		opcode := hdr[0] & 0x0f
		masked := hdr[1]&0x80 != 0
		length := int(hdr[1] & 0x7f)
		switch length {
		case 126:
			ext := make([]byte, 2)
			if _, err := io.ReadFull(c.conn, ext); err != nil {
				return nil, err
			}
			length = int(binary.BigEndian.Uint16(ext))
		case 127:
			ext := make([]byte, 8)
			if _, err := io.ReadFull(c.conn, ext); err != nil {
				return nil, err
			}
			length = int(binary.BigEndian.Uint64(ext))
		}
		var mask [4]byte
		if masked {
			if _, err := io.ReadFull(c.conn, mask[:]); err != nil {
				return nil, err
			}
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(c.conn, data); err != nil {
			return nil, err
		}
		if masked {
			for i := range data {
				data[i] ^= mask[i%4]
			}
		}
		switch opcode {
		case 0x8:
			return nil, io.EOF
		case 0x9:
			c.writeFrame(0xA, data)
			continue
		case 0x1, 0x2, 0x0:
			payload = append(payload, data...)
		}
		if fin {
			return payload, nil
		}
	}
}

func (c *wsConn) writeFrame(opcode byte, data []byte) error {
	header := []byte{0x80 | opcode}
	n := len(data)
	switch {
	case n < 126:
		header = append(header, byte(n))
	case n <= 65535:
		header = append(header, 126, 0, 0)
		binary.BigEndian.PutUint16(header[len(header)-2:], uint16(n))
	default:
		header = append(header, 127)
		ext := make([]byte, 8)
		binary.BigEndian.PutUint64(ext, uint64(n))
		header = append(header, ext...)
	}
	if _, err := c.conn.Write(header); err != nil {
		return err
	}
	_, err := c.conn.Write(data)
	return err
}

func (c *wsConn) writeText(data []byte) error { return c.writeFrame(0x1, data) }

func (c *wsConn) close() error { return c.conn.Close() }

func randomKey() string {
	b := make([]byte, 16)
	rand.Read(b)
	return base64.StdEncoding.EncodeToString(b)
}
