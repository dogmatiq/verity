package mlog

import (
	"fmt"
	"io"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/iago/must"
)

const (
	// TransactionIDIcon is the icon shown directly before a transaction ID. It
	// is a circle with a dot in the center, intended to be reminiscent of an
	// electron circling a nucleus, indicating "atomicity". There is a unicode
	// atom symbol, however it does not tend to be discernable at smaller font
	// sizes.
	TransactionIDIcon Icon = "⨀"

	// MessageIDIcon is the icon shown directly before a message ID.
	// It is an "equals sign", indicating that this message "has exactly" the
	// displayed ID.
	MessageIDIcon Icon = "="

	// CausationIDIcon is the icon shown directly before a message causation ID.
	// It is the mathematical "because" symbol, indicating that this message
	// happened "because of" the displayed ID.
	CausationIDIcon Icon = "∵"

	// CorrelationIDIcon is the icon shown directly before a message correlation
	// ID. It is the mathematical "member of set" symbol, indicating that this
	// message belongs to the set of messages that came about because of the
	// displayed ID.
	CorrelationIDIcon Icon = "⋲"

	// ConsumeIcon is the icon shown to indicate that a message is being
	// consumed. It is a downward pointing arrow, as such "inbound" messages
	// could be considered as being "downloaded" from the network or queue.
	ConsumeIcon Icon = "▼"

	// ConsumeErrorIcon is a variant of ConsumeIcon used when there is an error
	// condition. It is an hollow version of the regular consume icon,
	// indicating that the requirement remains "unfulfilled".
	ConsumeErrorIcon Icon = "▽"

	// ProduceIcon is the icon shown to indicate that a message is being
	// produce. It is an upward pointing arrow, as such "outbound" messages
	// could be considered as being "uploaded" to the network or queue.
	ProduceIcon Icon = "▲"

	// ProduceErrorIcon is a variant of ProduceIcon used when there is an error
	// condition. It is an hollow version of the regular produce icon,
	// indicating that the requirement remains "unfulfilled".
	ProduceErrorIcon Icon = "△"

	// RetryIcon is an icon used instead of ConsumeIcon when a message is being
	// re-attempted. It is an open-circle with an arrow, indicating that the
	// message has "come around again".
	RetryIcon Icon = "↻"

	// ErrorIcon is the icon shown when logging information about an error.
	// It is a heavy cross, indicating a failure.
	ErrorIcon Icon = "✖"

	// AggregateIcon is the icon shown when a log message relates to an aggregate
	// message handler. It is the mathematical "therefore" symbol, representing the
	// decision making as a result of the message.
	AggregateIcon Icon = "∴"

	// ProcessIcon is the icon shown when a log message relates to a process
	// message handler. It is three horizontal lines, representing the step in a
	// process.
	ProcessIcon Icon = "≡"

	// IntegrationIcon is the icon shown when a log message relates to an
	// integration message handler. It is the relational algebra "join" symbol,
	// representing the integration of two systems.
	IntegrationIcon Icon = "⨝"

	// ProjectionIcon is the icon shown when a log message relates to a projection
	// message handler. It is the mathematical "sum" symbol , representing the
	// aggregation of events.
	ProjectionIcon Icon = "Σ"

	// SystemIcon is an icon shown when a log message relates to the internals of
	// the engine. It is a sprocket, representing the inner workings of the
	// machine.
	SystemIcon Icon = "⚙"

	// SeparatorIcon is an icon used to separate strings of unrelated text inside a
	// log message. It is a large bullet, intended to have a large visual impact.
	SeparatorIcon Icon = "●"
)

// Icon is a unicode symbol used as an icon in log messages.
type Icon string

func (i Icon) String() string {
	return string(i)
}

// WriteTo writes a string representation of the icon to w.
// If i is the zero-value, a single space is rendered.
func (i Icon) WriteTo(w io.Writer) (int64, error) {
	s := i.String()
	if i == "" {
		s = " "
	}

	n, err := io.WriteString(w, s)
	return int64(n), err
}

// WithLabel return an IconWithLabel containing this icon and the given label.
func (i Icon) WithLabel(f string, v ...interface{}) IconWithLabel {
	return IconWithLabel{
		i,
		formatLabel(fmt.Sprintf(f, v...)),
	}
}

// WithID return an IconWithLabel containing this icon and an ID as its label.
//
// The id is formatted using FormatID().
func (i Icon) WithID(id string) IconWithLabel {
	return i.WithLabel(FormatID(id))
}

// IconWithLabel is a container for an icon and its associated text label.
type IconWithLabel struct {
	Icon  Icon
	Label string
}

func (i IconWithLabel) String() string {
	return i.Icon.String() + " " + i.Label
}

// WriteTo writes a string representation of the icon and its label to w.
func (i IconWithLabel) WriteTo(w io.Writer) (_ int64, err error) {
	defer must.Recover(&err)

	n := must.WriteTo(w, i.Icon)
	n += must.Write(w, space1)
	n += must.WriteString(w, i.Label)

	return int64(n), err
}

// formatLabel formats a label for display.
func formatLabel(label string) string {
	if label == "" {
		return "-"
	}

	return label
}

// HandlerTypeIcon returns the icon to use for the given handler type.
func HandlerTypeIcon(t configkit.HandlerType) Icon {
	t.MustValidate()

	switch t {
	case configkit.AggregateHandlerType:
		return AggregateIcon
	case configkit.ProcessHandlerType:
		return ProcessIcon
	case configkit.IntegrationHandlerType:
		return IntegrationIcon
	default: // configkit.ProjectionHandlerType:
		return ProjectionIcon
	}
}
