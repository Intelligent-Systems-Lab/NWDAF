package consumer

import "testing"

func TestAdrfClientForTargetReusesNormalizedTarget(t *testing.T) {
	consumer := newConsumerWithServices(nil, nil)

	first := consumer.adrfClientForTarget(" http://adrf.example/ ")
	second := consumer.adrfClientForTarget("http://adrf.example")
	other := consumer.adrfClientForTarget("http://other-adrf.example")

	if first != second {
		t.Fatal("equivalent ADRF targets did not reuse one transport client")
	}
	if first == other {
		t.Fatal("different ADRF targets reused the same transport client")
	}
}
