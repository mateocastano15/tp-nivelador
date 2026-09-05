package client

import (
	"os"
	"io"
	"bytes"

	"github.com/7574-sistemas-distribuidos/tp-nivelador/src/logger"
)

type FileHandler struct{
	nextLine []byte
	line []byte
	inputFile *os.File
	outputFile *os.File
}

func (fileHandler *FileHandler) writeResponse(data []byte) (error){
	data = append(data, FILE_WRITER_DELIMITER)
	_, err := fileHandler.outputFile.Write(data)

	if err!= nil{
		logger.Error("writing-file", logger.Fail, data)
		return err
	}

	return nil
}

func (fileHandler *FileHandler) readLine() (error){
	buf := make([]byte, FILE_READER_BUFFER_SIZE)
	fileHandler.line = fileHandler.nextLine
	for bytes.IndexByte(fileHandler.line, FILE_READER_DELIMITER) == -1 {
		n, err := fileHandler.inputFile.Read(buf)
		fileHandler.line = append(fileHandler.line, buf[:n]...)

		if err != nil && err != io.EOF {
			logger.Error("read-file", logger.Fail, fileHandler.inputFile)
			return err
		}

		if err == io.EOF {
			if bytes.IndexByte(fileHandler.line, FILE_READER_DELIMITER) == -1 {
				return io.EOF
			}
			break
		}
	}

	endOfLine := bytes.IndexByte(fileHandler.line, FILE_READER_DELIMITER)
	fileHandler.nextLine = fileHandler.line[endOfLine+1:]
	fileHandler.line = bytes.TrimSuffix(fileHandler.line[:endOfLine], []byte{CARRIAGE_RETURN})

	return nil
}

func NewFileHandler(config ClientConfig) (*FileHandler, error) {
	inputFile, err := os.Open(config.InputFile)

	if err != nil {
		logger.Error("open-file", logger.Fail, config.InputFile)
		return nil, err
	}

	outputFile, err := os.Create(config.OutputFIle)

	if err != nil {
		logger.Error("open-file", logger.Fail, config.OutputFIle)
		inputFile.Close()
		return nil, err
	}

	var nextLine []byte
	var line []byte

	fileHandler := &FileHandler{inputFile: inputFile, nextLine: nextLine, line:line, outputFile: outputFile}
	return fileHandler, nil
}