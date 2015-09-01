package logicsocket

import (
	"bytes"
	"encoding/binary"
	"log"
	"net"
	"sync"
)

type LogicReaderWriter struct {
	readBuff []byte
	name     int32
	lc       *LogicConnection
}

type req struct {
	name int32
	data []byte
	done chan error
}

type LogicConnection struct {
	write chan req
	read  map[int32]chan []byte
	lock  sync.Mutex
	conn  net.Conn
}

func Wrap(c net.Conn) *LogicConnection {
	res := &LogicConnection{
		write: make(chan req),
		read:  make(map[int32]chan []byte),
		conn:  c,
	}
	go res.doRead()
	go res.doWrite()
	return res
}

func (lc *LogicConnection) getReadChan(name int32) (res chan []byte) {
	lc.lock.Lock()
	defer lc.lock.Unlock()
	var ok bool
	if res, ok = lc.read[name]; !ok {
		res = make(chan []byte)
		lc.read[name] = res
	}
	return res
}

func (lc *LogicConnection) removeReadChan(name int32) {
	lc.lock.Lock()
	defer lc.lock.Unlock()
	delete(lc.read, name)
}

func (lc *LogicConnection) doRead() {
	for {
		buffer := make([]byte, 8)
		var totalread int32 = 0
		for totalread < 8 {
			n, err := lc.conn.Read(buffer[totalread:])
			if err != nil {
				log.Fatal("doRead 0 ", err, n)
				return
			}
			totalread += int32(n)
		}
		reader := bytes.NewReader(buffer)
		var name int32
		var size int32
		err := binary.Read(reader, binary.BigEndian, &name)
		if err != nil {
			log.Fatal("doRead 1 ", err)
			return
		}
		err = binary.Read(reader, binary.BigEndian, &size)
		if err != nil {
			log.Fatal("doRead 2 ", err)
			return
		}
		buffer = make([]byte, size)
		totalread = 0
		for totalread < size {
			n, err := lc.conn.Read(buffer[totalread:])
			if err != nil {
				log.Fatal("doRead 3", err)
				return
			}
			totalread += int32(n)
		}
		c := lc.getReadChan(name)
		c <- buffer
	}
}

func (lc *LogicConnection) doWrite() {
	for {
		req := <-lc.write
		b := &bytes.Buffer{}
		err := binary.Write(b, binary.BigEndian, req.name)
		if err != nil {
			log.Fatal(err)
			req.done <- err
			continue
		}
		err = binary.Write(b, binary.BigEndian, int32(len(req.data)))
		if err != nil {
			log.Fatal(err)
			req.done <- err
			continue
		}
		_, err = lc.conn.Write(append(b.Bytes(), req.data...))
		req.done <- err
	}
}

func (lc *LogicConnection) NewConnection(name int32) *LogicReaderWriter {
	return &LogicReaderWriter{
		readBuff: make([]byte, 0),
		name:     name,
		lc:       lc,
	}
}

func (lrw *LogicReaderWriter) Read(p []byte) (n int, err error) {
	c := lrw.lc.getReadChan(lrw.name)
	if len(lrw.readBuff) == 0 {
		lrw.readBuff = append(lrw.readBuff, <-c...)
	}
	n = copy(p, lrw.readBuff)
	lrw.readBuff = lrw.readBuff[n:]
	return n, nil
}

func (lrw *LogicReaderWriter) Write(p []byte) (n int, err error) {
	r := req{
		name: lrw.name,
		data: p,
		done: make(chan error),
	}
	lrw.lc.write <- r
	err = <-r.done
	if err != nil {
		return 0, err
	} else {
		return len(p), nil
	}
}

func (lrw *LogicReaderWriter) Close() error {

}
