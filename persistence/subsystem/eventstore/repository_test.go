package eventstore_test

import (
	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type UnknownMessageError", func() {
	Describe("func Error()", func() {
		It("includes the message ID of the unknown message", func() {
			env := infixfixtures.NewEnvelope("<message-0>", dogmafixtures.MessageA1)
			err := UnknownMessageError{
				MessageID: env.MetaData.MessageId,
			}

			Expect(err).To(
				MatchError("message with ID '" + env.MetaData.MessageId + "' cannot be found"),
			)
		})
	})
})
