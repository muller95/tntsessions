package tntsessions

import (
	"time"

	"fmt"

	"github.com/google/uuid"
	tarantool "github.com/tarantool/go-tarantool"
)

//SessionsBase represents conntection to tarantool database
type SessionsBase struct {
	conn  *tarantool.Connection
	space string
}

//Represents session of web-application
type Session struct {
	ID      string
	EndTime int64
	Data    map[string]interface{}
}

//ConnectToTarantool tries to create connection to the given address using user name and password
func ConnectToTarantool(addr, user, password, space string) (*SessionsBase, error) {
	var err error

	sessDb := new(SessionsBase)
	opts := tarantool.Opts{
		Timeout:       50 * time.Millisecond,
		Reconnect:     100 * time.Millisecond,
		MaxReconnects: 3,
		User:          user,
		Pass:          password,
	}

	sessDb.conn, err = tarantool.Connect(addr, opts)
	if err != nil {
		return nil, fmt.Errorf("Err on connecting to tarantool: %v", err)
	}

	sessDb.space = space
	return sessDb, nil
}

//CreateSession creates new session in the given base with the given data,
//returns session struct or error
func (sessDb *SessionsBase) Create(lifetime int64) (*Session, error) {
	sid, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("Err on creating session id: %v", err)
	}

	sess := new(Session)
	sess.ID = sid.String()
	sess.EndTime = time.Now().Unix() + lifetime
	sess.Data = make(map[string]interface{})

	_, err = sessDb.conn.Insert(sessDb.space, []interface{}{sess.ID, sess.EndTime, sess.Data})
	if err != nil {
		return nil, fmt.Errorf("Err on inserting session: %v", err)
	}

	return sess, nil
}
