package main

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	s3Client *s3.S3
	s3Bucket string
	wg       *sync.WaitGroup
)

func init() {
	sess, err := session.NewSession(
		&aws.Config{
			Region: aws.String("us-east-1"),
			Credentials: credentials.NewStaticCredentials(
				"",
				"",
				"",
			),
		},
	)
	if err != nil {
		panic(err)
	}

	s3Client = s3.New(sess)
	s3Bucket = "goexpert-remonato-bucket"
}

func main() {
	dir, err := os.Open("./tmp")
	if err != nil {
		panic(err)
	}

	defer dir.Close()
	uploadControl := make(chan struct{}, 50) //Limitando a 50 goroutines simultâneas
	errorFileUpload := make(chan string, 10) //Canal para capturar erros de upload

	go func() {
		for {
			select {
			case fileName := <-errorFileUpload:
				uploadControl <- struct{}{}
				wg.Add(1)
				go uploadFile(fileName, uploadControl, errorFileUpload)
			}
		}
	}()

	for {
		files, err := dir.ReadDir(1)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Error reading directory: %v\n", err)
			continue
		}
		wg.Add(1)
		uploadControl <- struct{}{} //Adiciona uma struct vazia ao canal para controlar a concorrência
		go uploadFile(files[0].Name(), uploadControl, errorFileUpload)
	}
	wg.Wait()
}

func uploadFile(fileName string, uploadControl <-chan struct{}, errorFileUpload chan<- string) {
	wg.Done()
	completFileName := fmt.Sprintf("./tmp/%s", fileName)
	f, err := os.Open(completFileName)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", completFileName, err)
		<-uploadControl                    // Remove uma struct do canal para liberar espaço
		errorFileUpload <- completFileName // Envia o nome do arquivo para o canal de erro
		return
	}

	defer f.Close()

	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(fileName),
		Body:   f,
	})

	if err != nil {
		fmt.Printf("Error uploading file %s: %v\n", fileName, err)
		<-uploadControl // Remove uma struct do canal para liberar espaço
		errorFileUpload <- completFileName
		return
	}

	fmt.Printf("File %s uploaded successfully to bucket %s\n", fileName, s3Bucket)
	<-uploadControl // Remove uma struct do canal para liberar espaço

}
