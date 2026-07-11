package contract

import "fmt"

type ModelIdentity struct {
	ProviderID    string `json:"provider_id"`
	ModelUniqueID int64  `json:"model_unique_id"`
}

func (m ModelIdentity) Key() string {
	return fmt.Sprintf("%s/%d", m.ProviderID, m.ModelUniqueID)
}

func (m ModelIdentity) Valid() bool {
	return m.ProviderID != "" && m.ModelUniqueID >= 0
}
