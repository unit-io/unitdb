# Unit Transport Protocol (uTP)

## About uTP
The uTP (unit Tranport Protocol) specification defines the Client Server message transport protocol. It is light weight, open, simple and designed for communication in Machine to Machine (M2M) and Internet connected devices (IoT) contexts. The uTP protocol runs over TCP/IP, WebSocket, GRPC or other network protocols that provide bi-directional connections.

## Message Flow
An application transport the data by uTP across network, it contains payload data, delivey mode and topic with optional collection of properties.

### Client
A Client opens the network connection to the Server using TCP/IP, WebSocket, GRPC or other bi-direction network protocols.
- Pubslihes Application Mesasges to a topic that other Clients subscribes in.
- Subscribes to a topic to receive Application Messages.
- Unsubcribe to remove a topic subscription.
- Closes the network connection to the Server.

### Server
- Accepts the network connections from Clients.
- Recieves and store Application Messages published by Clients.
- Processes topic subscription requests from Clients.
- Route Application Messages that match Client subscriptions.
- Closes the network connection from the Client.

## Application Message
The Application Messages are transported between Client and Server in the form of data Packets. A data Packet consist of Fixed Header and a uTP Message Type defined in the specification.

### Fixed Header
Each uTP data packet contains a Fixed Header as shown below:

|  Name | Type |
| :--- | :--- |
| MessageType |	enum |
| MessageLength | int32 |

### Message Type

| Name | Value | Direction of Flow |
| :--- | :--- | :--- |
| RERSERVED | 0 | Forbidden |
| CONNECT |	1 |	Client to Server |
| CONNACK |	2 |	Server to Client |
| PUBLISH |	3 |	Client to Server or Server to Client |
| PUBNEW | 4 | Server to Client |
| PUBRECEIVE | 5 |	Client to Server |
| PUBRECEIPT | 6 | Client to Server or Server to Client |
| PUBCOMPLETE |	7 | Client to Server or Server to Client |
| SUBSCRIBE | 8 | Client to Server |
| SUBACK | 9 | Server to Client |
| UNSUBSCRIBE |	10	| Client to Server |
| UNSUBACK | 11 | Server to Client |
| PINGREQ |	12 | Client to Server |
| PINGRESP | 13 | Server to Client |
| DISCONNECT | 14 | Client to Server or Server to Client |

## Delivery Mode
The uTP delivers Application Messages as per the Delivery Mode defined in the publish and subscribe Message type. The Client and Server both can be a publisher or subcriber of messages.
Th Delivery Mode in the oubound Application Message to the Client could differ from that of inbound Application Message.

### Express Delivery
The Express Delivery Mode ensures that the Application Message arrives at the receiver at least once. An Express Delivery Mode PUBLISH Message has a Message Identifier and get the receipt acknowledgement by a PUBRECEIPT Message.

In the Express Delivery Mode, the sender
- Must send a PUBLISH Message containing a Message Identifier with Delivery Mode Express
- Must treat a PUBLISH Message as "unacknowledged" until it has received the corresponding PUBCOMPLETE Message from the receiver.

A sender is permitted to send further PUBLISH Messages with different Message Identifier while it is waiting to receive publish receipts.

In the Express Delivery Mode, the receiver
- Must respond with a PUBCOMPLETE Message containing the Message Identifier from the incoming PUBLISH Message.

### Reliable Delivery
The Reliable Delivery Mode ensures no duplication of Application Messages delivered to the receivers. There is an increased overhead associated to the Reliable Delivery Mode.

### Inbound Message Flow from a Client
In the Reliable Delivery Mode, the sender
- Must send a PUBLISH Message containing a Message Identifier with Delivery Mode Reliable
- Must treat a PUBLISH Message as "unacknowledged" until it has received the corresponding PUBRECEIPT Message from the receiver.
- Must send a PUBCOMPLETE Message when it receive a PUBRECEIPT Message from the receiver. This PUBCOMPLETE Message must contain the same Message Identifier from original PUBLISH Message.
- Must NOT re-send the PUBLISH Message once it has received the corresponding PUBRECEIPT Message.

A sender is permitted to send further PUBLISH Messages with different Message Identifier while it is waiting to receive publish receipts.

In the Reliable Delivery Mode, the receiver
- Must respond with a PUBRECEIPT containing the Message Identifier from the incoming PUBLISH Message.
- Until it has received a corresponding PUBCOMPLETE Message, the receiver MUST acknowledge any subsequent PUBLISH Message with the same Message Identifier by sending a PUBRECEIPT Message.

### Outbound Message Flow to the Client
In the Reliable Delivery Mode, the sender
- Must send a PUBNEW Message containing a Message Identifier
- Must send PUBLISH Message when it receive a PUBRECEIVE Packet from the receiver. This PUBLISH Message must conrain the same Message Identifier from PUBNEW Message.
- Must treat a PUBLISH Message as "unacknowledged" until it has received the corresponding PUBRECEIPT Message from the receiver.
- Must send a PUBCOMPLETE Message when it receive a PUBRECEIPT Message from the receiver. This PUBCOMPLETE Message must contain the same Message Identifier from original PUBNEW Message.
- Must NOT re-send the PUBLISH Message once it has received the corresponding PUBRECEIPT Message.

A sender is permitted to send further PUBLISH Messages with different Message Identifier while it is waiting to receive publish receipts.

In the Reliable Delivery Mode, the receiver
- Must respond with a PUBRECEIVE containing the Message Identifier from the incoming PUBNEW Message.
- Must respond with a PUBRECEIPT Message when it receive a PUBLISH Message from the sender containing the Message Identifier from the incoming PUBNEW Message.
- Until it has received a corresponding PUBCOMPLETE Message, the receiver MUST acknowledge any subsequent PUBLISH Message with the same Message Identifier by sending a PUBRECEIPT Message.

