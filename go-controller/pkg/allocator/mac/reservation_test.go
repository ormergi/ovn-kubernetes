package mac_test

import (
	"errors"
	"net"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/allocator/mac"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("ReservationManager", func() {
	const testNetwork = "test-network"
	const owner1 = "namespace1/pod1"
	const owner2 = "namespace2/pod2"

	var tracker *mac.ReservationManager
	var mac1, mac2 net.HardwareAddr

	BeforeEach(func() {
		var err error
		mac1, err = net.ParseMAC("aa:bb:cc:dd:ee:f1")
		Expect(err).NotTo(HaveOccurred())
		mac2, err = net.ParseMAC("aa:bb:cc:dd:ee:f2")
		Expect(err).NotTo(HaveOccurred())
		tracker = mac.NewManager()
	})

	Context("reserve", func() {
		DescribeTable("should handle given zero value gracefully",
			func(network, owner string, mac net.HardwareAddr) {
				Expect(tracker.Reserve(network, owner, mac)).To(Succeed())
			},
			Entry("", "", "", nil),
			Entry("", testNetwork, "", nil),
			Entry("", "", owner1, nil),
			Entry("", "", "", mac1),
			Entry("", testNetwork, owner1, nil),
			Entry("", testNetwork, "", mac1),
			Entry("", "", owner1, mac1),
		)
		It("should allow adding same MAC multiple times without duplicates", func() {
			Expect(tracker.Reserve(testNetwork, owner1, mac1)).To(Succeed())
		})
		It("should not fail on repeated reservation", func() {
			Expect(tracker.Reserve(testNetwork, owner1, mac1)).To(Succeed())
			Expect(tracker.Reserve(testNetwork, owner1, mac1)).To(Succeed(), "same owner should not raise a conflict")
			err := tracker.Reserve(testNetwork, owner2, mac1)
			Expect(err).To(HaveOccurred(), "different owner should raise a conflict")
			Expect(errors.Is(err, mac.ErrMACConflict)).To(BeTrue())
		})
		It("should isolate MACs between different networks", func() {
			const anotherNetwork = "another-network"

			Expect(tracker.Reserve(testNetwork, owner1, mac1)).To(Succeed())
			Expect(tracker.Reserve(anotherNetwork, owner1, mac2)).To(Succeed())

			// MAC1 should conflict only in test-network
			Expect(tracker.Reserve(testNetwork, owner2, mac1)).ToNot(Succeed(), "mac1 already reserved in testNetwork for owner1")
			Expect(tracker.Reserve(anotherNetwork, owner2, mac1)).To(Succeed(), "mac1 not reserved in anotherNetwork")
			// MAC2 should conflict only in another-network
			Expect(tracker.Reserve(testNetwork, owner2, mac2)).To(Succeed(), "mac2 not reserved ion testNetwork")
			Expect(tracker.Reserve(anotherNetwork, owner2, mac2)).ToNot(Succeed(), "mac2 already reserved in anotherNetwork for owner2")
		})
	})

	Context("release", func() {
		DescribeTable("should handle given zero values gracefully",
			func(network, owner string, mac net.HardwareAddr) {
				Expect(tracker.Release(network, owner, mac)).To(Succeed())
			},
			Entry("", "", "", nil),
			Entry("", testNetwork, "", nil),
			Entry("", "", owner1, nil),
			Entry("", "", "", mac1),
			Entry("", testNetwork, owner1, nil),
			Entry("", testNetwork, "", mac1),
			Entry("", "", owner1, mac1),
		)

		It("should successfully remove existing MAC", func() {
			Expect(tracker.Reserve(testNetwork, owner1, mac1)).To(Succeed(), "should reserve mac1 for owner1")
			Expect(tracker.Reserve(testNetwork, owner2, mac1)).ToNot(Succeed(), "should fail because mac1 should be reserved by owner1")

			Expect(tracker.Release(testNetwork, owner1, mac1)).To(Succeed(), "should release mac1")

			Expect(tracker.Reserve(testNetwork, owner2, mac1)).To(Succeed(), "should reserved mac1 as it should be free")
		})
	})
})
