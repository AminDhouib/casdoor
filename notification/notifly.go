// Copyright 2026 The Casdoor Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	notify "github.com/casdoor/notify2"
)

const notiflyTriggerUrl = "https://api.notifly.io/v1/events/trigger"

type notiflyService struct {
	apiKey       string
	workflow     string
	subscriberId string
}

type notiflyResponse struct {
	Message interface{} `json:"message"`
	Data    struct {
		Acknowledged bool     `json:"acknowledged"`
		Status       string   `json:"status"`
		Error        []string `json:"error"`
	} `json:"data"`
}

// NewNotiflyProvider triggers a Notifly workflow, which delivers the message to the subscriber
// over the channels set up in that workflow.
func NewNotiflyProvider(apiKey string, workflow string, subscriberId string) (notify.Notifier, error) {
	return &notiflyService{
		apiKey:       apiKey,
		workflow:     workflow,
		subscriberId: subscriberId,
	}, nil
}

func (s *notiflyService) Send(ctx context.Context, subject, content string) error {
	return s.SendWithRecipient(ctx, subject, content, "")
}

// SendWithRecipient sends to the given subscriber instead of the one set on the provider
func (s *notiflyService) SendWithRecipient(ctx context.Context, subject, content, recipient string) error {
	if ctx == nil {
		ctx = context.Background()
	}

	subscriberId := s.subscriberId
	if recipient != "" {
		subscriberId = recipient
	}

	if subject == "" {
		subject = "Casdoor"
	}

	message := map[string]interface{}{
		"name": s.workflow,
		"to":   map[string]string{"subscriberId": subscriberId},
		"payload": map[string]string{
			"title": subject,
			"body":  content,
		},
	}

	jsonData, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal Notifly request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", notiflyTriggerUrl, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create Notifly request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "ApiKey "+s.apiKey)

	resp, err := getCustomHttpNotificationClient().Do(req)
	if err != nil {
		return fmt.Errorf("failed to send Notifly request: %w", err)
	}
	defer resp.Body.Close()

	var notiflyResp notiflyResponse
	decodeErr := json.NewDecoder(resp.Body).Decode(&notiflyResp)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("Notifly returned HTTP status %d: %v", resp.StatusCode, notiflyResp.Message)
	}
	if decodeErr != nil {
		return fmt.Errorf("failed to decode Notifly response: %w", decodeErr)
	}
	if !notiflyResp.Data.Acknowledged || notiflyResp.Data.Status != "processed" {
		if len(notiflyResp.Data.Error) > 0 {
			return fmt.Errorf("Notifly did not process the event, status: %s, error: %v", notiflyResp.Data.Status, notiflyResp.Data.Error)
		}
		return fmt.Errorf("Notifly did not process the event, status: %s", notiflyResp.Data.Status)
	}

	return nil
}
