
## Simple Distributed Hash Table (SDHT)

This is an implementation of a distributed hash table using Chord protocol 

#### Building Docker containers

Build the RootNode

`docker build -f RootContainer/Dockerfile . -t sdht/server:1.0`

Build the ChordNode

`build -f ChordContainer/Dockerfile . -t sdht/client:1.0`

#### Running the SDHT

1. Start the Root node 

`
docker run -it --rm  --network=host  sdht/server:1.0`

This will start the root node at Port `6423`

2. Start Each Chord node by changing the PORT value to any number between 6000 - 7000

`docker run -it --rm --network=host -e ROOTHOST="0.0.0.0" -e ROOTPORT=6423 -e PORT=6425 sdht/client:1.0`



