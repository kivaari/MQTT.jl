import MQTT.User

@info "Running smoke tests"

condition = Condition()
topic = "foo"
payload = randstring(20)

function on_msg(t, p)
    @info "Received message topic: [", t, "] payload: [", String(copy(p)), "]"
    @test t == topic
    @test String(copy(p))== payload

    notify(condition)
end

client = Client(on_msg)

@info "Testing reconnect"
connect(client, "test.mosquitto.org")
disconnect(client)
connect(client, "test.mosquitto.org")

subscribe(client, (topic, QOS_0))

@info "Testing publish qos 0"
publish(client, topic, payload, qos=QOS_0)
wait(condition)

@info "Testing publish qos 1"
publish(client, topic, payload, qos=QOS_1)
wait(condition)

@info "Testing publish qos 2"
publish(client, topic, payload, qos=QOS_2)
wait(condition)

@info "Testing connect will"
disconnect(client)
connect(client, "test.mosquitto.org", will=Message(false, 0x00, false, topic, payload))

disconnect(client)
