# `linear` branch

This branch contains the **most modern version of the Riposte code**, which is used in the variant of Riposte that appears in Henry Corrigan-Gibbs' [PhD dissertation](https://purl.stanford.edu/nm483fv2043). This is the cleanest and fastest variant of the scheme and you should use this version unless you have a good reason to prefer the historical ones from the original Riposte paper.



An explanation of the branches in this repository is [here](https://bitbucket.org/henrycg/riposte/).


## How to build


1. Make sure that you have `go` installed:
```
go version
```

2. Clone the repository:
```
git clone https://bitbucket.org/henrycg/riposte/
```

3. Build the `client` and `server` binaries:
```
cd riposte
cd client 
go build
cd ..
cd server
go build
cd ..
```

4. Now you should be able to run 
```
server/server -help
client/client -help
```
to run the client and server and see the command-line options.

## AWS ALB mode (HTTPS RPC)

This codebase now supports two RPC transports:

1. `tls` (default): legacy RPC over custom TLS/TCP.
2. `https`: net/rpc over HTTPS, which is compatible with AWS ALB HTTPS listeners and HTTPS target groups.

To run with ALB-compatible transport, pass `-rpc-transport https` to both server and client:

```
server/server -idx 0 -servers "10.0.1.10:8000,10.0.1.11:8001" -rpc-transport https
server/server -idx 1 -servers "10.0.1.10:8000,10.0.1.11:8001" -rpc-transport https
client/client -leader "your-alb-dns-name:443" -rpc-transport https
```

Notes:

1. Server-to-server communication also uses the selected transport.
2. For ALB target groups, use protocol HTTPS and forward to the instance port your server listens on.
3. If ALB terminates TLS at the listener and uses plain HTTP to targets, this mode is not appropriate.
4. Use `/healthz` for ALB health checks.
5. Client upload RPCs are leader-only, so client-facing ALB routing should target the leader server.
