package api_test

import (
	"context"

	. "github.com/dogmatiq/infix/api"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ Discoverer = &StaticDiscoverer{}

var _ = Describe("type StaticDiscoverer", func() {
	var disc *StaticDiscoverer

	BeforeEach(func() {
		disc = &StaticDiscoverer{
			Hosts:   []string{"foo:100", "bar:200"},
			Options: []grpc.DialOption{grpc.WithInsecure()},
		}
	})

	Describe("func Discover()", func() {
		It("returns connections for each host", func() {
			conns, err := disc.Discover(context.Background())
			Expect(err).ShouldNot(HaveOccurred())

			var targets []string
			for _, conn := range conns {
				targets = append(targets, conn.Target())
			}

			Expect(targets).To(ConsistOf("foo:100", "bar:200"))
		})
	})
})
