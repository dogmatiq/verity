package retry_test

import (
	"errors"
	"math"
	"time"

	. "github.com/dogmatiq/infix/retry"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type ExponentialBackoff", func() {
	var (
		now   time.Time
		rp    ExponentialBackoff
		cause []error
	)

	BeforeEach(func() {
		now = time.Now()
		rp = ExponentialBackoff{
			Min: 100 * time.Millisecond,
			Max: 1 * time.Hour,
		}
		cause = []error{
			errors.New("<error>"),
		}
	})

	It("uses the minimum delay on the first failure", func() {
		next := rp.NextRetry(now, 0, cause)
		delay := next.Sub(now)

		Expect(delay).To(Equal(100 * time.Millisecond))
	})

	It("increases the delay with subsequent failures", func() {
		var next time.Time

		for retries := 0; retries <= 5; retries++ {
			n := rp.NextRetry(now, retries, cause)
			Expect(n).To(BeTemporally(">", next))

			next = n
		}
	})

	It("caps the delay at the maximum delay", func() {
		next := rp.NextRetry(now, math.MaxUint32, cause)
		delay := next.Sub(now)

		Expect(delay).To(Equal(1 * time.Hour))
	})

	It("supports random jitter", func() {
		rp.Jitter = 0.1

		next := rp.NextRetry(now, 0, cause)
		Expect(next).To(BeTemporally("~", now.Add(100*time.Millisecond), 10*time.Millisecond))

		for i := 0; i < 100; i++ {
			n := rp.NextRetry(now, 0, cause)
			if !n.Equal(next) {
				return
			}
		}

		Fail("100 iterations returned results with no jitter")
	})
})
