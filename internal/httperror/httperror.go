package httperror

import (
	"fmt"
	"net/http"
)

type Error struct {
	status int
	format string
	args   []any
}

func New(status int) error {
	return Newf(status, http.StatusText(status))
}

func Newf(status int, format string, args ...any) error {
	return &Error{
		status: status,
		format: format,
		args:   args,
	}
}

func (err Error) Error() string {
	return fmt.Sprintf(err.format, err.args...)
}

func (err Error) Code() int {
	return err.status
}

func Write(w http.ResponseWriter, err error) {
	httpErr, ok := err.(*Error)
	if !ok {
		http.Error(w, err.Error(), 500)
		return
	}
	http.Error(w, httpErr.Error(), httpErr.Code())
}
