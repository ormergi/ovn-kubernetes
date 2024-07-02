package template

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("NetAttachDefTemplate", func() {
	It("should fail", func() {
		r := NetAttachDefTemplate{}
		_, err := r.RenderNetAttachDefManifest(nil)
		Expect(err).To(HaveOccurred())
	})
})
