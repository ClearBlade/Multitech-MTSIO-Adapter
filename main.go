package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	cb "github.com/clearblade/Go-SDK"
	mqttTypes "github.com/clearblade/mqtt_parsing"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/logutils"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

const (
	platURL         = "http://localhost:9000"
	messURL         = "localhost:1883"
	msgSubscribeQos = 0
	msgPublishQos   = 0
	mtsIoRead       = "read"
	mtsIoWrite      = "write"
)

var (
	platformURL         string //Defaults to http://localhost:9000
	messagingURL        string //Defaults to localhost:1883
	sysKey              string
	sysSec              string
	deviceName          string //Defaults to mtsIoAdapter
	activeKey           string
	logLevel            string //Defaults to info
	adapterConfigCollID string

	topicRoot = "wayside/mtsio"

	cbBroker           cbPlatformBroker
	cbSubscribeChannel <-chan *mqttTypes.Publish
	endWorkersChannel  chan string
)

type cbPlatformBroker struct {
	name         string
	clientID     string
	client       *cb.DeviceClient
	platformURL  *string
	messagingURL *string
	systemKey    *string
	systemSecret *string
	username     *string
	password     *string
	topic        string
	qos          int
}

func init() {
	flag.StringVar(&sysKey, "systemKey", "", "system key (required)")
	flag.StringVar(&sysSec, "systemSecret", "", "system secret (required)")
	flag.StringVar(&deviceName, "deviceName", "mtsIoAdapter", "name of device (optional)")
	flag.StringVar(&activeKey, "password", "", "password (or active key) for device authentication (required)")
	flag.StringVar(&platformURL, "platformURL", platURL, "platform url (optional)")
	flag.StringVar(&messagingURL, "messagingURL", messURL, "messaging URL (optional)")
	flag.StringVar(&logLevel, "logLevel", "info", "The level of logging to use. Available levels are 'debug, 'info', 'warn', 'error', 'fatal' (optional)")

	flag.StringVar(&adapterConfigCollID, "adapterConfigCollectionID", "", "The ID of the data collection used to house adapter configuration (required)")
}

func usage() {
	log.Printf("Usage: mtsioAdapter [options]\n\n")
	flag.PrintDefaults()
}

func validateFlags() {
	flag.Parse()

	if sysKey == "" || sysSec == "" || activeKey == "" || adapterConfigCollID == "" {

		log.Printf("ERROR - Missing required flags\n\n")
		flag.Usage()
		os.Exit(1)
	}
}

func main() {
	fmt.Println("Starting mtsioAdapter...")

	//Validate the command line flags
	flag.Usage = usage
	validateFlags()

	//Initialize the logging mechanism
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"},
		MinLevel: logutils.LogLevel(strings.ToUpper(logLevel)),
		Writer: &lumberjack.Logger{
			Filename:   "/var/log/mtsioAdapter",
			MaxSize:    10, // megabytes
			MaxBackups: 5,
			MaxAge:     28, //days
		},
	}
	log.SetOutput(filter)

	cbBroker = cbPlatformBroker{

		name:         "ClearBlade",
		clientID:     deviceName + "_client",
		client:       nil,
		platformURL:  &platformURL,
		messagingURL: &messagingURL,
		systemKey:    &sysKey,
		systemSecret: &sysSec,
		username:     &deviceName,
		password:     &activeKey,
		//		topic:        "wayside/serial/#",
		qos: msgSubscribeQos,
	}

	// Initialize ClearBlade Client
	var err error
	if err = initCbClient(cbBroker); err != nil {
		log.Println(err.Error())
		log.Println("Unable to initialize CB broker client. Exiting.")
		return
	}

	defer close(endWorkersChannel)
	endWorkersChannel = make(chan string)
	done := make(chan bool)

	<-done
}

// ClearBlade Client init helper
func initCbClient(platformBroker cbPlatformBroker) error {
	log.Println("[DEBUG] initCbClient - Initializing the ClearBlade client")

	cbBroker.client = cb.NewDeviceClientWithAddrs(*(platformBroker.platformURL), *(platformBroker.messagingURL), *(platformBroker.systemKey), *(platformBroker.systemSecret), *(platformBroker.username), *(platformBroker.password))

	for err := cbBroker.client.Authenticate(); err != nil; {
		log.Printf("[ERROR] initCbClient - Error authenticating %s: %s\n", platformBroker.name, err.Error())
		log.Println("[ERROR] initCbClient - Will retry in 1 minute...")

		// sleep 1 minute
		time.Sleep(time.Duration(time.Minute * 1))
		err = cbBroker.client.Authenticate()
	}

	//Retrieve adapter configuration data
	log.Println("[INFO] main - Retrieving adapter configuration...")
	getAdapterConfig()

	log.Println("[DEBUG] initCbClient - Initializing MQTT")
	callbacks := cb.Callbacks{OnConnectionLostCallback: OnConnectLost, OnConnectCallback: OnConnect}
	if err := cbBroker.client.InitializeMQTTWithCallback(platformBroker.clientID, "", 30, nil, nil, &callbacks); err != nil {
		log.Fatalf("[FATAL] initCbClient - Unable to initialize MQTT connection with %s: %s", platformBroker.name, err.Error())
		return err
	}

	return nil
}

//If the connection to the broker is lost, we need to reconnect and
//re-establish all of the subscriptions
func OnConnectLost(client mqtt.Client, connerr error) {
	log.Printf("[INFO] OnConnectLost - Connection to broker was lost: %s\n", connerr.Error())

	//End the existing goRoutines
	endWorkersChannel <- "Stop Channel"

	//We don't need to worry about manally re-initializing the mqtt client. The auto reconnect logic will
	//automatically try and reconnect. The reconnect interval could be as much as 20 minutes.
}

//When the connection to the broker is complete, set up the subscriptions
func OnConnect(client mqtt.Client) {
	log.Println("[INFO] OnConnect - Connected to ClearBlade Platform MQTT broker")

	//CleanSession, by default, is set to true. This results in non-durable subscriptions.
	//We therefore need to re-subscribe
	log.Println("[DEBUG] OnConnect - Begin Configuring Subscription(s)")

	var err error
	for cbSubscribeChannel, err = subscribe(topicRoot + "/+/request"); err != nil; {
		//Wait 30 seconds and retry
		log.Printf("[ERROR] OnConnect - Error subscribing to MQTT: %s\n", err.Error())
		log.Println("[ERROR] OnConnect - Will retry in 30 seconds...")
		time.Sleep(time.Duration(30 * time.Second))
		cbSubscribeChannel, err = subscribe(topicRoot + "/#")
	}

	//Start subscribe worker
	go subscribeWorker()
}

func subscribeWorker() {
	log.Println("[DEBUG] subscribeWorker - Starting subscribeWorker")

	//Wait for subscriptions to be received
	for {
		select {
		case message, ok := <-cbSubscribeChannel:
			if ok {

				//Determine if a read or write request was received
				if strings.HasSuffix(message.Topic.Whole, mtsIoRead+"/request") {
					log.Println("[DEBUG] subscribeWorker - Read request received")
					// 1. Invoke appropriate read command
					log.Printf("[DEBUG] subscribeWorker - Executing mts-io-sysfs %s command\n", MTSIO_READ)
					executeCommands(MTSIO_READ, message.Payload)
				} else if strings.HasSuffix(message.Topic.Whole, mtsIoWrite+"/request") {
					log.Printf("[DEBUG] subscribeWorker - Write request received: %#v\n", message.Payload)
					// Execute appropriate write command
					log.Printf("[DEBUG] subscribeWorker - Executing mts-io-sysfs %s command\n", MTSIO_WRITE)
					executeCommands(MTSIO_WRITE, message.Payload)
				} else {
					log.Printf("[DEBUG] subscribeWorker - Unknown request received: topic = %s, payload = %#v\n", message.Topic.Whole, message.Payload)
				}
			}
		case _ = <-endWorkersChannel:
			//End the current go routine when the stop signal is received
			log.Println("[INFO] subscribeWorker - Stopping subscribeWorker")
			return
		}
	}
}

func executeCommands(operation string, payload []byte) {
	// The json request should resemble the following:
	//
	// {
	//      "portName": "gpiob",
	//		"objects": [
	//			{
	//				"name": "dout0",
	//				"value": ""
	//			},
	//			{
	//				"name": "dout1",
	//				"value": ""
	//			}
	//			...
	//		]
	// }

	log.Printf("[DEBUG] executeCommand - Json payload received: %#s\n", string(payload))

	var jsonPayload map[string]interface{}
	var jsonResp map[string]interface{}

	if err := json.Unmarshal(payload, &jsonPayload); err != nil {
		log.Printf("[ERROR] executeCommand - Error encountered unmarshalling json: %s\n", err.Error())
		jsonResp = createJsonError("Error encountered unmarshalling json: " + err.Error())
	} else {
		log.Printf("[DEBUG] executeCommand - Json payload received: %#v\n", jsonPayload)
	}

	//Validate the operation argument
	if jsonResp == nil {
		if operation == "" {
			log.Println("[ERROR] executeCommand - Operation not specified")
			jsonResp = createJsonError("Operation is required")
		} else {
			if operation != MTSIO_READ && operation != MTSIO_WRITE {
				log.Printf("[ERROR] executeCommand - Invalid operation specified: %s\n", operation)
				jsonResp = createJsonError("Invalid operation specified")
			}
		}
	}

	//Validate the portName argument
	if jsonResp == nil {
		if jsonPayload["objects"] == nil {
			log.Println("[ERROR] executeCommand - objects array not specified in incoming payload")
			jsonResp = createJsonError("The objects array is required")
		}
	}

	//Validate the payload argument
	if jsonResp == nil {
		if jsonPayload["portName"] == nil {
			log.Println("[ERROR] executeCommand - portName not specified in incoming payload")
			jsonResp = createJsonError("portName is required")
		}

		if jsonPayload["objects"] == nil {
			log.Println("[ERROR] executeCommand - objects array not specified in incoming payload")
			jsonResp = createJsonError("The objects array is required")
		}
	}

	if jsonResp == nil {
		objects := jsonPayload["objects"].([]interface{})

		for _, v := range objects {
			log.Printf("[DEBUG] executeCommand - Executing command: %#v\n", createCommandArgs(operation, jsonPayload["portName"].(string), v.(map[string]interface{})))
			cmd := exec.Command(MTSIO_CMD, createCommandArgs(operation, jsonPayload["portName"].(string), v.(map[string]interface{}))...)
			var out bytes.Buffer
			cmd.Stdout = &out

			if err := cmd.Run(); err != nil {
				log.Printf("[ERROR] executeCommand - ERROR executing mts-io-sysfs command: %s\n", err.Error())
				jsonResp = createJsonError(err.Error())
				break
			} else {
				log.Printf("[DEBUG] executeCommand - Command response received: %s\n", out.String())
				if operation == MTSIO_READ {
					v.(map[string]interface{})["value"] = convertResponseValue(strings.Replace(out.String(), "\n", "", -1))
				}
			}
		}

		if jsonResp == nil {
			jsonResp = jsonPayload
			jsonResp["success"] = true
		} else {
			jsonResp["portName"] = jsonPayload["portName"]
			jsonResp["objects"] = jsonPayload["objects"]
		}
		//Add a timestamp to the payload
		jsonResp["timestamp"] = time.Now().UTC().Format(time.RFC3339)

		log.Printf("[DEBUG] executeCommand - Marshalling response %#v\n", jsonResp)
		respStr, err := json.Marshal(jsonResp)
		if err != nil {
			log.Printf("[ERROR] executeCommand - ERROR marshalling json response: %s\n", err.Error())
		} else {
			log.Printf("[DEBUG] executeCommand - respStr = %#v\n", respStr)

			//Create the response topic
			theTopic := topicRoot + "/"
			if operation == MTSIO_READ {
				theTopic += mtsIoRead
			} else {
				theTopic += mtsIoWrite
			}

			theTopic += "/response"

			log.Printf("[DEBUG] executeCommand - Publishing response %s to topic %s\n", string(respStr), theTopic)

			//Publish the response
			err = publish(theTopic, string(respStr))
			if err != nil {
				log.Printf("[ERROR] subscribeWorker - ERROR publishing to topic: %s\n", err.Error())
			}
		}
	}
}

func createCommandArgs(operation string, portName string, object map[string]interface{}) []string {
	var cmdArgs = []string{operation, portName + "/" + object["name"].(string)}

	//Only append values to the command on write requests
	if operation == MTSIO_WRITE {
		if object["value"] != nil {
			switch object["value"].(type) {
			case string:
				cmdArgs = append(cmdArgs, object["value"].(string))
			case float64:
				cmdArgs = append(cmdArgs, strconv.FormatFloat(object["value"].(float64), 'E', -1, 64))
			case bool:
				if object["value"].(bool) {
					cmdArgs = append(cmdArgs, strconv.Itoa(MTSIO_ON))
				} else {
					cmdArgs = append(cmdArgs, strconv.Itoa(MTSIO_OFF))
				}

			}
		}
	}
	return cmdArgs
}

func convertResponseValue(respVal string) interface{} {
	if numVal, err := strconv.ParseFloat(respVal, 64); err == nil {
		if numVal == MTSIO_ON {
			return true
		}
		if numVal == MTSIO_OFF {
			return false
		}
		return numVal
	} else {
		return respVal
	}
}

func createJsonError(errMsg string) map[string]interface{} {
	err := make(map[string]interface{})
	err["success"] = false
	err["error"] = errMsg

	return err
}

// Subscribes to a topic
func subscribe(topic string) (<-chan *mqttTypes.Publish, error) {
	log.Printf("[DEBUG] subscribe - Subscribing to topic %s\n", topic)
	subscription, error := cbBroker.client.Subscribe(topic, cbBroker.qos)
	if error != nil {
		log.Printf("[ERROR] subscribe - Unable to subscribe to topic: %s due to error: %s\n", topic, error.Error())
		return nil, error
	}

	log.Printf("[DEBUG] subscribe - Successfully subscribed to = %s\n", topic)
	return subscription, nil
}

// Publishes data to a topic
func publish(topic string, data string) error {
	log.Printf("[DEBUG] publish - Publishing to topic %s\n", topic)
	error := cbBroker.client.Publish(topic, []byte(data), cbBroker.qos)
	if error != nil {
		log.Printf("[ERROR] publish - Unable to publish to topic: %s due to error: %s\n", topic, error.Error())
		return error
	}

	log.Printf("[DEBUG] publish - Successfully published message to = %s\n", topic)
	return nil
}

func getAdapterConfig() {
	log.Println("[INFO] getAdapterConfig - Retrieving adapter config")

	//Retrieve the adapter configuration row
	query := cb.NewQuery()
	query.EqualTo("adapter_name", "mtsIoAdapter")

	//A nil query results in all rows being returned
	log.Println("[DEBUG] getAdapterConfig - Executing query against table " + adapterConfigCollID)
	results, err := cbBroker.client.GetData(adapterConfigCollID, query)
	if err != nil {
		log.Println("[DEBUG] getAdapterConfig - Adapter configuration could not be retrieved. Using defaults")
		log.Printf("[DEBUG] getAdapterConfig - Error: %s\n", err.Error())
	} else {
		if len(results["DATA"].([]interface{})) > 0 {
			log.Printf("[DEBUG] getAdapterConfig - Adapter config retrieved: %#v\n", results)
			log.Println("[INFO] getAdapterConfig - Adapter config retrieved")

			//topic root
			if results["DATA"].([]interface{})[0].(map[string]interface{})["topic_root"] != nil {
				log.Printf("[DEBUG] getAdapterConfig - Setting topicRoot to %s\n", results["DATA"].([]interface{})[0].(map[string]interface{})["topic_root"].(string))
				topicRoot = results["DATA"].([]interface{})[0].(map[string]interface{})["topic_root"].(string)
			} else {
				log.Printf("[DEBUG] getAdapterConfig - Topic root is nil. Using default value %s\n", topicRoot)
			}
		} else {
			log.Println("[DEBUG] getAdapterConfig - No rows returned. Using defaults")
		}
	}
}
