package safe_socket

import "io"

//TODO: Complete with a short-read/short-write tolerant implementation

func SendAll(socket io.Writer, bytes []byte) error {
	n, err := socket.Write(bytes)
	if err != nil {
		return err
	}
	if n != len(bytes){
		return SendAll(socket, bytes[n:])
	}
	return nil
}

func RecvAll(socket io.Reader, size int) ([]byte, error) {
	buff := make([]byte, size)
	n, err := socket.Read(buff)
	if err != nil {
		return nil, err
	}
	if n != size {
		rest, err := RecvAll(socket, size-n)
		if err != nil {
			return nil, err
		}
		return append(buff[:n], rest...), nil
	}
	return buff[:size], nil
}
