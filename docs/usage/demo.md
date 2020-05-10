# Demo
To access unitdb using websocket build unitd from source code using go get command.

> go get -u github.com/unit-io/unitd && unitd

Open [unitd.html](https://github.com/unit-io/unitd/blob/master/examples/html/unitd.html) under example/html folder in browser.

## Steps
- Generate Client ID
- Specify new client ID and connect to client
- Specify topics to subscribe/publish messages and generate key
- Specify key to the topics with separator '/' and subscribe to topic
- Specify message to send and publish to topic

### First Client
<p align="left">
  <img src="docs/img/client1.png" /> 
</p>

### Second Client
<p align="left">
  <img src="docs/img/client2.png" /> 
</p>