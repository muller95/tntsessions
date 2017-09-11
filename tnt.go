package tntsessions

import (
	"time"

	"fmt"

	"github.com/google/uuid"
	tarantool "github.com/tarantool/go-tarantool"
)

//ErrNotFound is returned by SessionsBase method Get if session with given ID was not found
var ErrNotFound = fmt.Errorf("Session not found")

//ErrSessionExpired is returned by SessionsBase method Get if session with given ID is expired
var ErrSessionExpired = fmt.Errorf("Session expired")

//SessionsBase represents conntection to tarantool database
type SessionsBase struct {
	conn  *tarantool.Connection
	space string
}

//Session struct for user space
type Session struct {
	ID      string
	EndTime int64
	data    map[string]interface{}
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

//Create function creates new session in the given base with the given data,
//returns session struct or error.
func (sessDb *SessionsBase) Create(lifetime int64) (*Session, error) {
	sid, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("Err on creating session id: %v", err)
	}

	sess := new(Session)
	sess.ID = sid.String()
	sess.EndTime = time.Now().Unix() + lifetime
	sess.data = make(map[string]interface{})

	return sess, nil
}

//Put stores session into tarantool space, if session with given id exists, new one replaces old,
//or error if occurs
func (sessDb *SessionsBase) Put(sess *Session) error {
	_, err := sessDb.conn.Replace(sessDb.space, []interface{}{sess.ID, sess.EndTime, sess.data})
	if err != nil {
		return fmt.Errorf("Err on inserting session: %v", err)
	}

	return nil
}

//Delete removes session with given ID from tarantool, or error if occurs
func (sessDb *SessionsBase) Delete(sessID string) error {
	_, err := sessDb.conn.Delete(sessDb.space, "primary", []interface{}{sessID})
	if err != nil {
		return fmt.Errorf("Err delete session: %v", err)
	}

	return nil
}

//Get returns session with the given ID, or error if occurs
func (sessDb *SessionsBase) Get(sessID string) (*Session, error) {
	sess := new(Session)
	resp, err := sessDb.conn.Select(sessDb.space, "primary", 0, 1, tarantool.IterEq,
		[]interface{}{sessID})
	if err != nil {
		return nil, fmt.Errorf("Err select session: %v", err)
	}

	if len(resp.Tuples()) == 0 {
		return nil, ErrNotFound
	}

	sess.ID = sessID
	sess.EndTime = int64(resp.Tuples()[0][1].(uint64))
	if time.Now().Unix() > sess.EndTime {
		sessDb.Delete(sessID)
		return nil, ErrSessionExpired
	}
	data := resp.Tuples()[0][2].(map[interface{}]interface{})
	sess.data = make(map[string]interface{})
	for k, v := range data {
		sess.data[k.(string)] = v
	}

	return sess, nil
}

//ResetLifetime updates lifetime of session with start point from now
func (sess *Session) ResetLifetime(lifetime int64) {
	sess.EndTime = time.Now().Unix() + lifetime
}

//Set stores value into session by the given key
func (sess *Session) Set(key string, value interface{}) {
	sess.data[key] = value
}

//GetString returns string if such exists on the key, or empty string
func (sess *Session) GetString(key string) string {
	res, ok := sess.data[key]
	if !ok {
		return ""
	}

	switch res.(type) {
	case string:
		return res.(string)
	default:
		return ""
	}
}
