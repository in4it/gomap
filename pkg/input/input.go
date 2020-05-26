package input

func NewInput(fileToProcess FileToProcess) Input {
	switch fileToProcess.fileType {
	case "parquet":
		return NewParquetFile(fileToProcess)
	case "localfile":
		return NewLocalFile(fileToProcess)
	case "s3file":
		return NewS3File(fileToProcess)
	}

	return nil
}

func NewFileToProcess(filename, fileType string, schema interface{}) FileToProcess {
	return FileToProcess{filename: filename, fileType: fileType, schema: schema}
}
