package quotastop

import "testing"

// hk 4625: a pane whose last agent activity is the provider's own
// credit-exhausted rejection names the pool, so the hub can start the
// exhausted-pool sweep for it. The Anthropic and OpenAI texts are the errors
// credit_rejection.go reads from an omp session; the screens here are their
// visible renders.
func TestCreditExhaustedReadsProviderRejection(t *testing.T) {
	for _, tc := range []struct {
		name, screen, pool string
		ok                 bool
	}{
		{
			"anthropic",
			"⏺ API Error: 400 Your credit balance is too low to access the Anthropic API\n\n❯ \n",
			"anthropic-api", true,
		},
		{
			"anthropic with a status prefix",
			"■ API Error: 400 {\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Your credit balance is too low to access the Anthropic API\"}}\n",
			"anthropic-api", true,
		},
		{
			"openai insufficient_quota",
			"⏺ API Error: 429 {\"error\":{\"code\":\"insufficient_quota\",\"message\":\"You exceeded your current quota\"}}\n\n❯ \n",
			"openai-api", true,
		},
		{
			"openai current quota",
			"Error: You exceeded your current quota, please check your plan and billing details.\n",
			"openai-api", true,
		},
		{
			// The same words in an answer after the rejection: not a stop.
			"quoted by an answer",
			"API Error: 400 Your credit balance is too low to access the Anthropic API\nI will retry with a smaller prompt.\n",
			"", false,
		},
		{
			"ordinary answer",
			"I pushed the fix and opened the PR.\n\n❯ \n",
			"", false,
		},
		{
			"quota stop is not a credit rejection",
			"You've hit your usage limit. Please try again at 12:39 PM.\n",
			"", false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, ok := CreditExhausted(tc.screen)
			if ok != tc.ok || pool != tc.pool {
				t.Fatalf("CreditExhausted(%q) = (%q, %t), want (%q, %t)", tc.screen, pool, ok, tc.pool, tc.ok)
			}
		})
	}
}
