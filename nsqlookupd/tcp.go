package nsqlookupd

import (
	"io"
	"net"
	"sync"

	"github.com/nsqio/nsq/internal/protocol"
)

type tcpServer struct {
	nsqlookupd *NSQLookupd
	conns      sync.Map
}

func (p *tcpServer) Handle(conn net.Conn) {
	p.nsqlookupd.logf(LOG_INFO, "TCP: new client(%s)", conn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "failed to read protocol version - %s", err)
		_ = conn.Close()
		return
	}
	protocolMagic := string(buf)

	p.nsqlookupd.logf(LOG_INFO, "CLIENT(%s): desired protocol magic '%s'",
		conn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	case "  V1":
		prot = &LookupProtocolV1{nsqlookupd: p.nsqlookupd}
	default:
		if _, err := protocol.SendResponse(conn, []byte("E_BAD_PROTOCOL")); err != nil {
			p.nsqlookupd.logf(LOG_ERROR, "failed to send bad protocol response to client(%s) - %s", conn.RemoteAddr(), err)
		}
		if err := conn.Close(); err != nil {
			p.nsqlookupd.logf(LOG_ERROR, "failed to close client(%s) connection - %s", conn.RemoteAddr(), err)
		}
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) bad protocol magic '%s'",
			conn.RemoteAddr(), protocolMagic)
		return
	}

	client := prot.NewClient(conn)
	p.conns.Store(conn.RemoteAddr(), client)

	err = prot.IOLoop(client)
	if err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "client(%s) - %s", conn.RemoteAddr(), err)
	}

	p.conns.Delete(conn.RemoteAddr())
	if err := client.Close(); err != nil {
		p.nsqlookupd.logf(LOG_ERROR, "failed to close client(%s) - %s", conn.RemoteAddr(), err)
	}
}

func (p *tcpServer) Close() {
	p.conns.Range(func(k, v interface{}) bool {
		if err := v.(protocol.Client).Close(); err != nil {
			p.nsqlookupd.logf(LOG_ERROR, "failed to close client(%s) - %s", k, err)
		}
		return true
	})
}
