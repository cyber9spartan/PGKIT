package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/skolio/pgkit/qb"
)

// Notification is a LISTEN/NOTIFY message received from PostgreSQL.
type Notification struct {
	Channel string
	Payload string
	PID     uint32
}

// Notify sends a notification to channel with payload.
// The channel name is safely quoted; payload is a bound parameter.
//
//	client.Notify(ctx, "orders", `{"id":"123"}`)
func (c *Client) Notify(ctx context.Context, channel, payload string) error {
	_, err := c.exec.ExecWrite(ctx, c.mustPool("write"),
		fmt.Sprintf("SELECT pg_notify(%s, $1)", qb.QuoteIdent(channel)),
		[]any{payload},
	)
	if err != nil {
		return fmt.Errorf("db: NOTIFY %s: %w", channel, err)
	}
	return nil
}

// Listen acquires a dedicated connection, issues LISTEN channel, and calls
// handler for every notification until ctx is cancelled.
//
// handler is called synchronously; return an error to stop listening.
//
//	err := client.Listen(ctx, "orders", func(n db.Notification) error {
//	    fmt.Println(n.Payload)
//	    return nil
//	})
func (c *Client) Listen(ctx context.Context, channel string, handler func(Notification) error) error {
	conn, err := c.mustPool("write").Acquire(ctx)
	if err != nil {
		return fmt.Errorf("db: acquire conn for LISTEN: %w", err)
	}
	defer conn.Release()

	rawConn := conn.Conn()
	if _, err = rawConn.Exec(ctx, "LISTEN "+qb.QuoteIdent(channel)); err != nil {
		return fmt.Errorf("db: LISTEN %s: %w", channel, err)
	}

	for {
		n, err := rawConn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // normal shutdown
			}
			return fmt.Errorf("db: WaitForNotification: %w", err)
		}
		if handlerErr := handler(fromPGNotification(n)); handlerErr != nil {
			return handlerErr
		}
	}
}

// ListenMulti listens on multiple channels simultaneously.
// Notifications from any channel are dispatched to handler.
func (c *Client) ListenMulti(ctx context.Context, channels []string, handler func(Notification) error) error {
	if len(channels) == 0 {
		return fmt.Errorf("db: ListenMulti requires at least one channel")
	}
	conn, err := c.mustPool("write").Acquire(ctx)
	if err != nil {
		return fmt.Errorf("db: acquire conn for LISTEN: %w", err)
	}
	defer conn.Release()

	rawConn := conn.Conn()
	for _, ch := range channels {
		if _, err = rawConn.Exec(ctx, "LISTEN "+qb.QuoteIdent(ch)); err != nil {
			return fmt.Errorf("db: LISTEN %s: %w", ch, err)
		}
	}

	for {
		n, err := rawConn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("db: WaitForNotification: %w", err)
		}
		if handlerErr := handler(fromPGNotification(n)); handlerErr != nil {
			return handlerErr
		}
	}
}

func fromPGNotification(n *pgconn.Notification) Notification {
	return Notification{Channel: n.Channel, Payload: n.Payload, PID: n.PID}
}
