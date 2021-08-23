# Kafka Streams POC

The purpose here is trying to get a better understanding of Kafka Streams, how data is partitioned and how to retrieve
it.

## Setup

Just run `mvn clean package` to create the jar file. Evrything else is controlled by the docker-compose.yml.

Every streams app instance can be configured with the following settings:

    SERVER_PORT: 9001

The port to use for our server.

    SPRING_PROFILES_ACTIVE: docker

The Spring profile to be activated (leave this to docker or add custom profiles)

    RUN_SIMULATORS: 'true'

Indicator whether you want this instance to start simulators. There are 2 simulators one to generate **Bills of Lading**
and one for generating **Vessel Visits**. They will both be published to their topics.

### Kafka

We're setting up a 3 broker Kafka cluster. Topics are created with 30 partitions.

### Application

We're also spinning up 5 instances of our kafka-streams app.

## What does it do?

Simulators publish new VesselVisits and new BillOfLadings to Kafka every x seconds. Now, the BillOfLadings all have a
link to some VesselVisit, but we don't know when the vesselvisit will be created.

How can we still process all the records and link the two together?

The Vessel Visits are put in a local storage which is accessible and can be queried by our REST Api.

The Bills of Lading will be put in a buffer in case their corresponding Vessel Visit is not known yet. A Punctuator runs
every X seconds and tries to match the buffered Bills of Lading with their Vessel Visits. If both are registered a new
output object is created and stored in a KTable and topics.

The simulators generate Vessel Visits with a configurable ID length. By using more characters for the ID more Bills of
lading will need to be buffered before they can be matched. Only alphanumeric values (a-z, A-Z, 0-9 = 62 characters) are
used for generating, which means a total combinations of:

| id length | combinations |
| --------- | ------------ |
| 1         | 62           |
| 2         | 3.844        |
| 3         | 238.328      |
| 4         | 14.776.336   |
| 5         | 916.132.832  |

## REST API

### Metadata

GET http://localhost:9003/api/metadata

Returns a list of metadata from the clients.

### All matched Bills of Lading including Vessel Visit

GET http://localhost:9001/api/bills-of-lading

returns the complete list of Bill Of Ladings with a matched Vessel Visit.

### Retrieve Vessel Visit

GET http://localhost:9002/api/vessel-visits/{id}

Returns the Vessel Visit if it exists.

### Retrieve buffer sizes

GET http://localhost:9002/api/bills-of-lading/buffers

Returns the current buffer sizes

## View logs

    docker-compose logs -f streams_app_1 streams_app_2 streams_app_3 streams_app_4 streams_app_5 | grep -i -E 'ERROR|Buffer contains|Removing BillOfLading'

# TODO

- make the buffer size visible
