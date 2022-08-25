package main

import (
	"math/rand"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/brocaar/chirpstack-api/go/v3/as/integration"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {

	opts := mqtt.NewClientOptions()
	opts.AddBroker("localhost:1883")
	opts.SetClientID("chirpstack-simualtor")
	opts.SetAutoReconnect(true)
	mqttClient := mqtt.NewClient(opts)
	for {
		log.Info("integration/mqtt: connecting to broker")
		if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
			log.Errorf("integration/mqtt: connecting to broker error, will retry in 2s: %s", token.Error())
			time.Sleep(2 * time.Second)
		} else {
			log.Info("integration/mqtt: successed to broker")
			break
		}
	}

	for i := 0; i < 200; i++ {
		sid := strconv.Itoa(i)
		num := rand.Int63n(30) + 5
		d := &Device{
			deviceId:   "device" + sid,
			topic:      "test/" + sid,
			mqttClient: mqttClient,
			timer:      time.NewTimer(time.Second * time.Duration(num)),
		}
		d.loop()
	}

	select {}

}

type Device struct {
	mqttClient mqtt.Client
	topic      string
	deviceId   string
	timer      *time.Timer
}

func (d *Device) loop() {

	go func() {
		for {
			select {
			case <-d.timer.C:
				d.send()
				d.timer.Reset(time.Second * 5)
			}
		}
	}()
}

func (d *Device) send() {
	up := &integration.UplinkEvent{
		DevEui: []byte(d.deviceId),
		Data:   []byte("01"),
		FCnt:   1,
		FPort:  1,
	}
	log.Infof("integration/mqtt: begin send mqtt %s to broker", d.topic)
	token := d.mqttClient.Publish(d.topic, 1, false, up.String())
	if token.Wait() && token.Error() != nil {
		log.Error("happen error", token.Error())
	}
}
