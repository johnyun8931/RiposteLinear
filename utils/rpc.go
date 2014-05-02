package utils

import (
  "crypto/tls"
  "log"
  "net"
  "net/rpc"
)

/* For running RPC over TLS. */

func ListenAndServe(address string, key_idx int, is_leader bool) {
  var config tls.Config
  if !is_leader {
    config.ClientAuth = tls.RequireAnyClientCert
  }
  config.InsecureSkipVerify = true
  config.Certificates = []tls.Certificate{ServerCertificates[key_idx]}

  l, err := tls.Listen("tcp", address, &config)
  if err != nil {
    log.Fatal("Listener error:", err)
    return
  }

  defer l.Close()

  for {
    conn, err := l.Accept()
    if err != nil {
      log.Printf("Listener error:", err)
      continue
    }

    go handleOneClient(conn)
  }
}

func handleOneClient(conn net.Conn) {
  defer conn.Close()

  tlscon, ok := conn.(*tls.Conn)
  if !ok {
    log.Printf("Could not cast conn")
    return
  }

  err := tlscon.Handshake()
  if err != nil {
    log.Printf("Handshake failed:", err)
    return
  }

  log.Printf("Handshake OK")

  rpc.ServeConn(conn)
}

func DialHTTPWithTLS(network, address string,
    client_idx int, server_idx int) (*rpc.Client, error) {
  var config tls.Config
  config.InsecureSkipVerify = true

  if client_idx >= 0 {
    config.Certificates = []tls.Certificate{ServerCertificates[client_idx]}
  }

  conn, err := tls.Dial(network, address, &config)
  if err != nil {
    log.Printf("DialHTTP error:", err)
    return nil, err
  }

  state := conn.ConnectionState()
  log.Printf("State: \n", state.PeerCertificates)

  return rpc.NewClient(conn), nil
}

