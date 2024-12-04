# Kakarot

## Resources

    great video: https://www.youtube.com/watch?v=NxhZ_c8YX8E
    another one: https://www.youtube.com/watch?v=1QdKhNpsj8M

    visualisation: https://kelseyc18.github.io/kademlia_vis/basics/

    Peer connectivity: https://jenkov.com/tutorials/p2p/peer-connectivity.html
    Peer addressability: https://jenkov.com/tutorials/p2p/peer-addressability.html


## Algorithms

### Chord (Uniring)

### Kademlia (Uniring)

### Tapestry (Uniring)
### Pastry (Uniring)

    Amount of peers in memory is adjustable. Better for low memory systems

### Polymorph Polyring (Polyring)

## RPC

### Message types

    required:

        2 byte string, generated by querying node and echoe'd by responding node
        t: transaction_id

        message type can be one of:
          - query
          - response
          - error
        y: q | r | e

    optional but recommended:

        2 byte string indicating client id
        v: <2 byte str>

    queries:

        # query type can be one of:
        #   - ping
        #   - store
        #   - find_node
        #   - find_key
        q: ping | store | find_node | find_key

        query arguments
        
            Ping:        a: {}
            store:
            find_node:   a: { "id": "querying_node_id", "target": "target_node_id" }


                # optional arguments for query type


