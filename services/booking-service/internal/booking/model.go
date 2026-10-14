package booking

import (
	"errors"
	"time"
)

type Status string

// Reservation state machine. Every transition is a conditional UPDATE
// (`WHERE status = <expected>`), so concurrent actors (user, expiry worker,
// payment reconciler) can never both win.
//
//	HELD ──confirm──▶ PAYMENT_PENDING ──charged──▶ CONFIRMED
//	 │                    │   ▲
//	 │                    │   └─provider unreachable (not charged)─▶ back to HELD
//	 │                    └──declined / not charged by deadline──▶ PAYMENT_FAILED (released)
//	 ├──cancel──▶ CANCELLED (released)
//	 └──hold TTL passed──▶ EXPIRED (released)
const (
	StatusHeld           Status = "HELD"
	StatusPaymentPending Status = "PAYMENT_PENDING"
	StatusConfirmed      Status = "CONFIRMED"
	StatusCancelled      Status = "CANCELLED"
	StatusExpired        Status = "EXPIRED"
	StatusPaymentFailed  Status = "PAYMENT_FAILED"
)

type Reservation struct {
	ID          string    `json:"id"`
	UserID      string    `json:"user_id"`
	EventID     int64     `json:"event_id"`
	TierID      int64     `json:"tier_id"`
	ShardNo     int       `json:"-"`
	Quantity    int       `json:"quantity"`
	AmountCents int64     `json:"amount_cents"`
	Currency    string    `json:"currency"`
	Status      Status    `json:"status"`
	PaymentRef  *string   `json:"payment_ref,omitempty"`
	ExpiresAt   time.Time `json:"expires_at"`
	Version     int       `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type ReserveRequest struct {
	UserID         string
	IdempotencyKey string
	EventID        int64 `json:"event_id"`
	TierID         int64 `json:"tier_id"`
	Quantity       int   `json:"quantity"`
}

var (
	ErrInvalidRequest      = errors.New("invalid request")
	ErrNotFound            = errors.New("not found")
	ErrSoldOut             = errors.New("sold out")
	ErrQuotaExceeded       = errors.New("per-user ticket limit reached")
	ErrSaleNotStarted      = errors.New("sale has not started")
	ErrReservationExpired  = errors.New("reservation is no longer payable")
	ErrInvalidState        = errors.New("reservation state does not allow this action")
	ErrPaymentInProgress   = errors.New("payment is being processed")
	ErrPaymentDeclined     = errors.New("payment declined")
	ErrPaymentUnavailable  = errors.New("payment provider unavailable, try again shortly")
	ErrOverloaded          = errors.New("system busy, retry later")
	ErrInventoryContention = errors.New("inventory busy, retry")
)
