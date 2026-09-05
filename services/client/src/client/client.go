package client

import (
	"net"
	"io"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/safe_socket"
)

const CONNECTION_ATTEMPTS_MAX = 3
const CONNECTION_ATTEMPS_DELAY_MS = 200

const FILE_READER_BUFFER_SIZE = 512
const FILE_READER_DELIMITER = '\n'
const FILE_WRITER_DELIMITER = '\n'
const CARRIAGE_RETURN = '\r'

type ClientConfig struct {
	ServerHost string
	ServerPort string
	AgencyId   string
	InputFile string
	OutputFIle string
}

type Client struct {
	conn   net.Conn
	config ClientConfig
}

func NewClient(config ClientConfig) (*Client, error) {
	conn, err := connectToServer(config.ServerHost, config.ServerPort)
	if err != nil {
		logger.Warn("connect-to-server", logger.Fail)
		return nil, err
	}

	client := &Client{conn: conn, config: config}
	return client, nil
}

func connectToServer(host, port string) (net.Conn, error) {
	const action = "connect-to-server"
	var err error
	var conn net.Conn

	logger.Info(action, logger.InProgress)
	for i := range CONNECTION_ATTEMPTS_MAX {
		conn, err = net.Dial("tcp", host+":"+port)
		if err != nil {
			logger.Warn(action, logger.Fail, "attempt", i)
			time.Sleep(CONNECTION_ATTEMPS_DELAY_MS * time.Millisecond)
			continue
		}

		logger.Info(action, logger.Success)
		break
	}

	return conn, err
}

func (client *Client) Run() error {
	defer client.conn.Close()
	fileHandler, err := NewFileHandler(client.config)
	if err != nil {
		logger.Error("read-file", logger.Fail, fileHandler)
		return err
	}
	defer fileHandler.inputFile.Close()
	defer fileHandler.outputFile.Close()

	for err = fileHandler.readLine(); err == nil || (err == io.EOF && len(fileHandler.line) > 0); err = fileHandler.readLine() {
		messageArgs := []any{"agency-id", client.config.AgencyId, "message", fileHandler.line}
		logger.Info("send-bet", logger.InProgress, messageArgs...)

		clientMessage := string(fileHandler.line)

		if err := safe_socket.SendAll(client.conn, []byte(clientMessage)); err != nil {
			logger.Error("send-message", logger.Fail, messageArgs...)
			return err
		}

		responseBuffer, err := safe_socket.RecvAll(client.conn, len(clientMessage))
		if err != nil {
			logger.Error("recv-response", logger.Fail, messageArgs...)
			return err
		}

		logger.Info("recv-response", logger.InProgress, messageArgs...)
		if err:= fileHandler.writeResponse(responseBuffer); err!= nil {
			logger.Error("write-response", logger.Fail, messageArgs...)
			return err
		}
	}
	if err != io.EOF {
		return err
	}

	logger.Info("read-file", logger.Success, "agency-id", client.config.AgencyId)

	return nil
}


