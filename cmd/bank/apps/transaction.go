package apps

import (
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/example/domain"
)

// TransactionApp is an implementation of dogma.Application for the bank example.
type TransactionApp struct {
}

// Configure configures the Dogma engine for this application.
func (a *TransactionApp) Configure(c dogma.ApplicationConfigurer) {
	c.Identity("transaction", "19c11eb2-432a-4d8c-b742-5866022379e2")

	c.RegisterAggregate(domain.DailyDebitLimitHandler{})
	c.RegisterAggregate(domain.TransactionHandler{})
	c.RegisterProcess(domain.DepositProcessHandler{})
	c.RegisterProcess(domain.TransferProcessHandler{})
	c.RegisterProcess(domain.WithdrawalProcessHandler{})
}
