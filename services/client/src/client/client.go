package client

import (
	"net"
	"io"
	"time"
	"bytes"

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

	if err := client.sendBets(fileHandler); err != nil {
		return err
	}

	bets, err := sendEndOfBets(client.conn, client.config.AgencyId)
	if err != nil {
		logger.Error("send-end-of-bets", logger.Fail, "agency-id", client.config.AgencyId)
		return err
	}

	logger.Info("recv-winners", logger.Success, "agency-id", client.config.AgencyId, "winners", len(bets.bets))

	for _, bet := range bets.bets {
		betLine := bytes.Join([][]byte{bet.firstName, bet.lastName, bet.document, bet.birthdate, bet.number}, []byte(","))
		if err := fileHandler.writeResponse(betLine); err != nil {
			logger.Error("write-response", logger.Fail, "agency-id", client.config.AgencyId)
			return err
		}
	}

	logger.Info("read-file", logger.Success, "agency-id", client.config.AgencyId)

	return nil
}

func (client *Client) sendBets(fileHandler *FileHandler) error {
	var err error
	for err = fileHandler.readLine(); err == nil || (err == io.EOF && len(fileHandler.line) > 0); err = fileHandler.readLine() {
		messageArgs := []any{"agency-id", client.config.AgencyId, "message", fileHandler.line}
		logger.Info("send-bet", logger.InProgress, messageArgs...)

		bet := createBet(fileHandler.line, client.config.AgencyId)

		if sendErr := sendBet(client.conn, bet); sendErr != nil {
			logger.Error("send-bet", logger.Fail, messageArgs...)
			return sendErr
		}

		logger.Info("send-bet", logger.Success, messageArgs...)
	}

	if err != io.EOF {
		return err
	}

	return nil
}

func receiveMessage(socket io.Reader) (Message, error) {
	header, err := safe_socket.RecvAll(socket, TYPE_BYTES+SIZE_BYTES)
	if err != nil {
		logger.Error("recv-type-and-size", logger.Fail)
		return nil, err
	}

	_, size := parseHeader(header)

	value, err := safe_socket.RecvAll(socket, size)
	if err != nil {
		logger.Error("recv-value", logger.Fail)
		return nil, err
	}

	return parseMessage(header, value)
}

func sendBet(conn io.ReadWriter, bet *Bet) error {
	if err := safe_socket.SendAll(conn, bet.byteBet); err != nil {
		return err
	}

	msg, err := receiveMessage(conn)
	if err != nil {
		return err
	}

	if _, ok := msg.(Ack); !ok {
		return ErrResponseMismatch{}
	}

	return nil
}

func sendEndOfBets(conn io.ReadWriter, agencyId string) (*Bets, error) {
	if err := safe_socket.SendAll(conn, encodeField(ENDOFBETS_BYTES, []byte(agencyId))); err != nil {
		return nil, err
	}

	msg, err := receiveMessage(conn)
	if err != nil {
		return nil, err
	}

	bets, ok := msg.(*Bets)
	if !ok {
		return nil, ErrResponseMismatch{}
	}

	return bets, nil
}
