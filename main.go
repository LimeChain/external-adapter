package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/hashgraph/hedera-sdk-go/v2"
	"github.com/joho/godotenv"
)

type JobResult struct {
	Id   string
	Data DataJson
}

type DataJson struct {
	Result        json.RawMessage
	HederaTopicId string
}

type Response struct {
	JobRunID string `json:"jobRunID"`
	Error    string `json:"error,omitempty"`
	Data     string `json:"data,omitempty"`
}

var hederaClient *hedera.Client

func externalAdapterHandler(res http.ResponseWriter, req *http.Request) {


	if req.URL.Path != "/" {
		http.Error(res, "404 not found.", http.StatusNotFound)
		return
	}

	if req.Method != http.MethodPost {
		http.Error(res, "405 Method not allowed.", http.StatusMethodNotAllowed)
		return
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(req.Body)
	
	var jobResult JobResult
	json.Unmarshal(buf.Bytes(), &jobResult)

	responseData := Response{}
	transactionReceipt, err := submitMessageToTopic(jobResult.Data.HederaTopicId, []byte(jobResult.Data.Result))
	if err != nil {
		log.Println("Error: ", err)
		responseData.Error = err.Error()
	} else {
		responseData.JobRunID = jobResult.Id
		responseData.Data = transactionReceipt.Status.String()
	}

	jsonResponse, err := json.Marshal(responseData)
	if (err != nil) {
		log.Println("Error: ", err)
		http.Error(res, "500 Internal Server Error.", http.StatusInternalServerError)
		return
	}

	fmt.Fprint(res, string(jsonResponse))
}

func submitMessageToTopic(hederaTopicId string, message []byte) (hedera.TransactionReceipt, error) {

	topicId, err := hedera.TopicIDFromString(hederaTopicId)

	if err != nil {
		return hedera.TransactionReceipt{}, err
	}

	transaction := hedera.NewTopicMessageSubmitTransaction().
		SetTopicID(topicId).
		SetMessage(message)

	txResponse, err := transaction.Execute(hederaClient)
	if err != nil {
		return hedera.TransactionReceipt{}, err
	}

	transactionReceipt, err := txResponse.GetReceipt(hederaClient)
	if err != nil {
		return hedera.TransactionReceipt{}, err
	}

	return transactionReceipt, nil
}

func main() {

	err := godotenv.Load(".env")
	if err != nil {
		panic(fmt.Errorf("Unable to load environment variables from .env file. Error:\n%v\n", err))
	}

	hederaAccountId, err := hedera.AccountIDFromString(os.Getenv("HEDERA_ACCOUNT_ID"))
	if err != nil {
		panic(err)
	}

	hederaPrivateKey, err := hedera.PrivateKeyFromString(os.Getenv("HEDERA_PRIVATE_KEY"))
	if err != nil {
		panic(err)
	}

	hederaClient = hedera.ClientForTestnet()
	hederaClient.SetOperator(hederaAccountId, hederaPrivateKey)

	http.HandleFunc("/", externalAdapterHandler)

	http.ListenAndServe(":8090", nil)
}
