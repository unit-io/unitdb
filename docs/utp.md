# Unit Transport Protocol (uTP)

## Table of Contents
 * [About uTP](#About-uTP)
 + [Message Flow](#Message-Flow)
   - [Client](#Client)
   - [Server](#Server)
 + [Application Message](#Application-Message)
   - [Fixed Header](#Fixed-Header)
   - [Message Type](#Message-Type)
   - [Flow Control](#Flow-Control)
 + [Session](#Session)
 + [Delivery Mode](#Delivery-Mode)
   - [Express Delivery](#Express-Delivery)
   - [Reliable and Batch Delivery ](#Reliable-and-Batch-Delivery )
   - [Inbound Message Flow from a Client](#Inbound-Message-Flow-from-a-Client)
   - [Outbound Message Flow to the Client](#Outbound-Message-Flow-to-the-Client)
   - [Batching](#Batching)
 + [Topics](#Topics)
   - [Topic Security](#Topic-Security)
   - [Topic Separator](#Topic-Separator)
   - [Wildcard Topic](#Wildcard-Topic)
   - [Multi-level Wildcard Topic](#Multi-level-Wildcard-Topic)
 + [Message Types](#Message-Types)
   - [CONNECT](#CONNECT---Connection-Request)
   - [PUBLISH](#PUBLISH---Publish-message)
   - [SUBSCRIBE](#SUBSCRIBE---Subscribe-request)
   - [UNSUBSCRIBE](#UNSUBSCRIBE---Unsubscribe-request)
   - [PINGREQ](#PINGREQ---PING-request)
   - [DISCONNECT](#DISCONNECT---Disconnect-notification)
 + [Control Message](#Control-Message)
   - [ACKNOWLEDGE](#ACKNOWLEDGE---acknowledgement)
   - [NOTIFY](#NOTIFY---New-message-notification)
   - [RECEIVE](#RECEIVE---ready-to-receive-message)
   - [RECEIPT](#RECEIPT---Publish-receipt)
   - [COMPLETE](#COMPLETE---Publish-complete)

## About uTP
The uTP (unit Tranport Protocol) specification defines the Client Server messaging transport that is agnostic of the content of the payload. It is light weight, open, simple and designed for communication in Machine to Machine (M2M) and Internet connected devices (IoT) contexts. The uTP protocol runs over TCP/IP, WebSocket, GRPC or other network protocols that provide bi-directional connections.

## Message Flow
An application transport the data by uTP across network, it contains payload data, delivey mode and topic with optional collection of properties.

### Client
A Client opens the network connection to the Server using TCP/IP, WebSocket, GRPC or other bi-direction network protocols.
- Pubslihes Application Mesasges to a topic that other Clients subscribes to.
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
The Application Messages are transported between Client and Server in the form of data Packets. A data Packet consist of Fixed Header and a uTP Message defined in the specification.

### Fixed Header
Each uTP data packet contains a Fixed Header as shown below. For Message Flow it contains Message Type, Flow Control and length of Message.

|  Name | Type |
| :--- | :--- |
| MessageType |	enum |
| Flow Control | enum |
| MessageLength | int32 |

### Message Type
Represented as enum value, the values are shown below.

| Name | Value | Direction of Flow |
| :--- | :--- | :--- |
| RERSERVED | 0 | Forbidden |
| CONNECT |	1 |	Client to Server |
| PUBLISH |	2 |	Client to Server or Server to Client |
| SUBSCRIBE | 3 | Client to Server |
| UNSUBSCRIBE |	4	| Client to Server |
| PINGREQ |	5 | Client to Server |
| DISCONNECT | 6 | Client to Server or Server to Client |

### Flow Control
Flow Control is Control Message sent in response to a uTP Message Type or another Control Message.

Client will send one of Message Type CONNECT, SUBSCRIBE, UNSUBSCRIBE, or PINGREQ  then Server will reponse with ACKNOWLEDGE Control Message. PUBLISH Message is sent from a Client to a Server or from a Server to a Client and if Delivery Mode is Express Delivery  then the receiver will respond with ACKNOWLEDGE Control Message. 

| Name | Value | Direction of Flow |
| :--- | :--- | :--- |
| None | 0 | None |
| ACKNOWLEDGE | 1 | Client to Server or Server to Client |
| NOTIFY | 2 | Server to Client |
| RECEIVE | 3 | Client to Server |
| RECEIPT | 4 | Client to Server or Server to Client |
| COMPLETE | 5 | Client to Server or Server to Client |

### Message Length
The Message Length represents number of bytes within the current Message.

### Message Identifier
All Message Types and Flow Control Message contains an int32 Message Identifier except CONNECT and DISCONNECT Message types.

## Session
In order to implement a Reliable Message Delivery flow the Client and Server need to associate state with the Client Identifier, this is referred to as the Session State. The Server also stores subscriptions in the Session State.

The Server persists an unique Session State per Client Identifier. Client can also specify Sesson Key during Connection to persist multiple Session States per Client Identifier. The Session can continue after network re-connection until Client specify Clean Session during a new Connection.

The Client Session pesist following States:
- Reliable and Batch Delivery messages sent to the Server, but have not been completely acknowledged.
- Reliable and Batch Delivery messages which have been received from the Server, but have not been completely acknowledged.

The Server Session persiste following states:
- The Client Subscriptions
- Reliable and Batch Delivery messages sent to the Client, but have not been completely acknowledged.
- Express, Reliable and Batch Delivery messages pending transmission to the Client.
- Reliable and Batch Delivery messages which have been received from the Client, but have not been completely acknowledged.

## Delivery Mode
The uTP delivers Application Messages according to the Delivery Mode defined in the Publish and Subscribe Message types. The delivery protocol is asymmetric, the Client and Server each can be either sender or receiver of Messages. The Delivery Mode to deliver an Outbound Application Message to the Client could differ from that of the Inbound Application Message.

### Express Delivery
The Express Delivery Mode ensures that the Application Message arrives at the receiver at least once. An Express Delivery Mode PUBLISH Message has a Message Identifier and get the acknowledgement by an ACKNOWLEDGE Control Message.

In the Express Delivery Mode, the sender
- Must send a PUBLISH Message containing a Message Identifier with Delivery Mode Express
- Must treat a PUBLISH Message as "unacknowledged" until it has received the corresponding ACKNOWLEDGE Control Message from the receiver.

A sender is permitted to send further PUBLISH Messages with different Message Identifier while it is waiting to receive publish receipts.

In the Express Delivery Mode, the receiver
- Must respond with an ACKNOWLEDGE Control Message containing the Message Identifier from the incoming PUBLISH Message.

### Reliable and Batch Delivery 
The Reliable and Batch Delivery Modes ensures no duplication of Application Messages delivered to the receivers. There is an increased overhead associated to the Reliable and Batch Delivery Modes.

### Inbound Message Flow from a Client
In the Reliable Delivery Mode, the sender
- Must send a PUBLISH Message containing a Message Identifier with Delivery Mode Reliable
- Must treat a PUBLISH Message as "unacknowledged" until it has received the corresponding ACKNOWLEDGE Control Message from the receiver.
- Must NOT re-send the PUBLISH Message once it has received the corresponding ACKNOWLEDGE Control Message.

A sender is permitted to send further PUBLISH Messages with different Message Identifier while it is waiting to receive publish receipts.

In the Reliable Delivery Mode, the receiver
- Must respond with a ACKNOWLEDGE containing the Message Identifier from the incoming PUBLISH Message.
- The receiver MUST acknowledge any subsequent PUBLISH Message with the same Message Identifier by sending a ACKNOWLEDGE Control Message.

### Outbound Message Flow to the Client
In the Reliable Delivery Mode, the sender
- Must send a NOTIFY Control Message containing a Message Identifier
- Must send PUBLISH Message when it receive a RECEIVE Control Message from the receiver. This PUBLISH Message must contain the same Message Identifier from NOTIFY Control Message.
- Must treat a PUBLISH Message as "unacknowledged" until it has received the corresponding RECEIPT Control Message from the receiver.
- Must send a COMPLETE Control Message when it receive a RECEIPT Control Message from the receiver. This COMPLETE Control Message must contain the same Message Identifier from original NOTIFY Control Message.
- Must NOT re-send the PUBLISH Message once it has received the corresponding RECEIPT Control Message.

A sender is permitted to send further PUBLISH Messages with different Message Identifier while it is waiting to receive publish receipts.

In the Reliable Delivery Mode, the receiver
- Must respond with a RECEIVE containing the Message Identifier from the incoming NOTIFY Control Message.
- Must respond with a RECEIPT Control Message when it receive a PUBLISH Message from the sender containing the Message Identifier from the incoming NOTIFY Control Message.
- Until it has received a corresponding COMPLETE Control Message, the receiver MUST acknowledge any subsequent PUBLISH Message with the same Message Identifier by sending a RECEIPT Control Message.

### Batching
The uTP protocol specifies the batching of Application Messages into a single call to the Server. The batching of messages provides better throughput, but the cost of batching is latency for Application Messages which are queued in memory until their corresponding batch is filled and ready to be sent over the network. The protocol flows for Batch Delivery Mode is similar to the Reliable Delivery Mode. The uTP protocol does not suport an Express Batch Delivery. 

Application Messages can be batched based on request size (in bytes), number of messages, and duration. The Client specifies the batching options during Connection, and also batch duration can be overriden with additional delay in Publish and Subcribe Message types. Batching of Application Messages are supported for both Publish and Subcribe Message types, and the Publish batching duration could differ from Subscribe batching duration. 

## Topics
### Topic Security
The Client may connect to the Server with a secure mode. If Client connects to the Server with secure mode then the Client must specify security key for Topic to Publish Messages or Subcribe to the Topic. A Security Key for a Topic must be prefix with Topic name using "/" separator. The Security Key must be UTF-8 Encoded String. Server must allow generating Security Key specific to the Topics when a Client makes a successful connection to the Server.

The Client may connect to the Server with Insecure mode then Security Key for Topic to Publish Messages or Sucbribe to the Topic is not required.

### Topic Separator
The dot ('.') is used to separate Topic and provide a hierarchical structure to the Topic Names. 

### Wildcard Topic
The asterisk sign ('*') is a wildcard character that matches only one Topic level.

The single level wildcard character can be used at any level in the Topic, including first and last levels. It can be used at more than one level in the Topic and can used in conjunction with the multi-level wildcard.

Following are the valid wildcard Topics:
- "*" is valid
- "teams.channel1.*" is valid
- "teams.*.userA" is valid

### Multi-level Wildcard Topic
The three dot sign ('...') is a wildcard character that matches any number of levels within a Topic. The multi-level wildcard represents parent and any number of child levels.

Following are valid multi-level wildcard Topics:
- "..." is valid
- "teams..." is valid
- "teams.channel1..." is valid
- "teams...userA" is not valid

## Message Types
### CONNECT - Connection Request
After a Client is successfully connected to a Server, the first Message sent from Client to the Server must be CONNECT Message.

A Client can send CONNECT Message once over a Network Connection.

The payload contains following fields.

|  Name | Type |
| :--- | :--- |
| Version | int32 |
| InsecureFlag | bool |
| ClientID | bytes |
| KeepAlive | int32 |
| CleanSessFlag | bool |
| SessKey | int32 |
| Username | bytes |
| Password | bytes |
| BatchDuration | int32 |
| BatchByteThreshold | int32 |
| BatchCountThreshold | int32 |

#### Version
The Protocol Version represents the revision level of the Protocol used by the Client.

A Server which suported multiple versions of the uTP Protocols uses the version to determine which version of uTP the Client is using.

#### Insecure Flag
Insecure Flag determine if security keys are required for publishing or subscribing to Topics. If Client connects with Server using Insecure Flag then security key prefix on a Topic is not required and Client and Server can Publish or Subscribe to a Topic without requiring a secuirty key.

#### Client Identifier
The ClientID identifies the Client to the Server. Each Client connecting to the Server has unique ClientID. the ClientID must be used by Clients and by Servers to identify state that they hold relating to the uTP Session between the Client and the Server.

The ClientID must be UTF-8 Encoded String as defined in [proto3](https://developers.google.com/protocol-buffers/docs/proto3) doc.

The ClientID is 52 UTF-8 encoded bytes in length, and that contains only the characters. If Server rejects ClientID it respond to the CONNECT Message with a CONNACK Message using Return Code 0x02 (Client Identifier not valid).

If ClientID sends an empty or invalid ClientID string in the CONNECT Message then the Server responds with a CONNACK Message using Return Code 0x06 (Unauthorized). 

#### Keep Alive
Keep alive time in seconds to keep the Client connection Open in case of no other uTP Message types has been received.

#### Clean Session Flag
Clean Session Flag indicates that the Session should start without using existing Session.

#### Session Key
Session Key is used to persist multiple Sessions per ClientID. By default Server persist an unique Session per ClientID.

#### User Name
User Name can be used by Server for authentication and authorization. It must be UTF-8 Encoded String as defined in [proto3](https://developers.google.com/protocol-buffers/docs/proto3) doc.

#### Password
Password field is used to carry any credentials. The Password field is binary data.

#### Batch Duration
Default batch duration to batch Application Messages based on duration for Delivery Mode of type Batch.

#### Batch Byte Threshold
Default batch byte threshold to batch Application Messages based on request size (in bytes) for Delivery Mode of type Batch or on Pubishing and Subscribing with delay delivery.

#### Batch Count Threshold
Default batch count threshold to batch Application Messages based on message count for Delivery Mode of type Batch or on Pubishing and Subscribing with delay delivery.

### PUBLISH - Publish message
A PUBLISH Message is sent from a Client to a Server or from a Server to a Client to transport an Application Message.

The payload contains following fields.

|  Name | Type |
| :--- | :--- |
| MessageID | int32 |
| DeliveryMode | int32 |
| Messages | repeated PublishMessage |

The PublishMessage contains folowing fields:

|  Name | Type |
| :--- | :--- |
| Topic | string |
| Payload | bytes |
| Ttl | string |

### TTL
A publisher can specify time to-live (TTL) when publishing an Application Message.

### SUBSCRIBE - Subscribe request
The SUBSCRIBE Message is sent from the Client to the Server to create one or more Subscriptions. Each Subscription registers one or more Topics for a Client. The Server sends PUBLISH Messages to the Client to forward Application Messages that were published to Topics that match these Subscriptions. The SUBSCRIBE Message also specifies (for each Subscription) the Delivery Mode with which the Server can send Application Messages to the Client.

The payload contains following fields.

|  Name | Type |
| :--- | :--- |
| MessageID | int32 |
| Subscriptions | repeated Subscription |

The Subscription contains folowing fields:

|  Name | Type |
| :--- | :--- |
| DeliveryMode | int32 |
| Delay | int32 |
| Topic | string |
| Last | string |

#### Last
A Subscriber can spcify Last duration (for example "1h") to retrive persisted Application Messages published to the subscribing Topic.

### UNSUBSCRIBE - Unsubscribe request
An UNSUBSCRIBE Message is sent by the Client to the Server, to unsubscribe from topics.

The payload contains following fields.

|  Name | Type |
| :--- | :--- |
| MessageID | int32 |
| Subscriptions | repeated Subscription |

### PINGREQ - PING request
The PINGREQ Message is sent from a Client to the Server to indicate to the Server that the Client is alive in the absence of any other uTP Messages being sent from the Client to the Server or request that the Server responds to confirm that it is alive.

This Message is used in Keep Alive processing.

### DISCONNECT - Disconnect notification
The DISCONNECT Message is the final uTP Message sent from the Client or the Server. It indicates the reason why the Network Connection is being closed. The Client or Server MAY send a DISCONNECT Message before closing the Network Connection.

## Control Message
### ACKNOWLEDGE - acknowledgement
The ACKNOWLEDGE Control Message is sent by the Server in response to CONNECT, SUBSCRIBE, UNSUBSCRIBE, PUBLISH or PINGREQ Message from a Client. 

The ACKNOWLEGE Message on CONNECT is sent with Message in the form of binary data that contains below Payload inforation. 

|  Name | Type |
| :--- | :--- |
| ReturnCode | int32 |
| Epoch | int32 |
| ConnID | int32 |

#### Connect Return Code
The Connect Return Code values are shown below. The Server must send a 0x00 (Success) Return Code before sending any Message. The Server must not send more than one CONNECT ACKNOWLEDGE in a Netwoork Connection.

If a Server sends a CONNECT ACKNOWLEDGE with Return Code other than 0x00 (Success) it must then close the Network Connection.

|  Hex | Return Code |
| :--- | :--- |
| 0x00 | connection accepted |
| 0x01 | unacceptable proto version |
| 0x02 | identifier rejected |
| 0x03 | unacceptable identifier, access not allowed |
| 0x04 | server unavailiable |
| 0x05 | not authorized |
| 0x06 | bad request |

#### Epoch
The Server must respond with CONNECT ACKNOWLEDGE containing Epoch. the Epoch value is used to cache the Session State in the Serve and in the Client if the Client request to persist multiple Sessions per Client Identifier.

#### Connection Indentifier
The Server must respond with a CONNECT ACKNOWLEDGE containing Connection Identifier. The assigned Connection Identifier must be a new Connection Indentifier not used by any other Session currently in the Server.

### NOTIFY - New message notification
The Server must send a NOTIFY Control Message containing a new Message Identifier if a sender Publishes a Message with RELIABLE or BATCH Delivery Mode and receiver has a matching subscription with RELIABLE or BATCH  Delivery Mode.

### RECEIVE - ready to receive message
The Client must send a RECEIVE Control Message to respond to the NOTIFY Control Message to tell Server it is ready to receive the message. It must contain the same Message Identifier from NOTIFY Control Message.

### RECEIPT - Publish receipt
The Server must respond with RECEIPT Control Message if it receive a PUBLISH Message with Delivery Mode as RELIABLE or BATCH Delivery. 

The Client must respond to a PUBLISH Message with a RECEIPT Control Message if it has a matching subscription with Delivery Mode RELIABLE or BATCH Delivery and sender has sent a PUBLISH Message with RELIABLE or BATCH Delivery Mode.

The RECEIPT Control Message must contain the same Message Identifier from PUBLISH Message.

### COMPLETE - Publish complete
The Client or the Server must respond with COMPLETE Control Message if it receives a RECEIPT Control Message to mark the Publish complete. The COMPLETE Control Message must contain the same Message Identifier from RECEIPT Control Message.
