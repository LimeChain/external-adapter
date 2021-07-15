package main

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	Result        string
	HederaTopicId string
}

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

	transactionReceipt := submitMessageToTopic(jobResult.Data.HederaTopicId, []byte(jobResult.Data.Result))

	fmt.Fprintf(res, "{\"transactionStatus\": \"/%v\"}", transactionReceipt.Status)

}

func submitMessageToTopic (hederaTopicId string, message []byte) hedera.TransactionReceipt {
	err := godotenv.Load(".env")
	if err != nil {
		panic(fmt.Errorf("Unable to load environment variables from .env file. Error:\n%v\n", err))
	}

	myAccountId, err := hedera.AccountIDFromString(os.Getenv("HEDERA_ACCOUNT_ID"))
	if err != nil {
		panic(err)
	}

	myPrivateKey, err := hedera.PrivateKeyFromString(os.Getenv("HEDERA_PRIVATE_KEY"))
	if err != nil {
		panic(err)
	}

	client := hedera.ClientForTestnet()
	client.SetOperator(myAccountId, myPrivateKey)

	topicId, err := hedera.TopicIDFromString(hederaTopicId)

	if err != nil {
		panic(err)
	}

	transaction := hedera.NewTopicMessageSubmitTransaction().
		SetTopicID(topicId).
		SetMessage(message)

	txResponse, err := transaction.Execute(client)
	if err != nil {
		panic(err)
	}

	transactionReceipt, err := txResponse.GetReceipt(client)
	if err != nil {
		panic(err)
	}

	return transactionReceipt
}


func main() {

	http.HandleFunc("/", externalAdapterHandler)

	http.ListenAndServe(":8090", nil)
}
